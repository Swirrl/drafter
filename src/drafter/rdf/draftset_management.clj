(ns drafter.rdf.draftset-management
  (:require [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [add s context]]
            [grafter.rdf.protocols :refer [map->Triple map->Quad]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.sesame :refer [read-statements]]
            [drafter.util :as util]
            [drafter.draftset :as ds]
            [drafter.user :as user]
            [drafter.util :as util]
            [drafter.rdf.draft-management :refer [update! query to-quads with-state-graph drafter-state-graph] :as mgmt]
            [drafter.rdf.draft-management.jobs :as jobs]
            [swirrl-server.async.jobs :refer [create-job create-child-job]]
            [drafter.write-scheduler :as scheduler]
            [schema.core :as s]
            [clojure.string :as string])
  (:import [java.util Date UUID]))

(defn- create-draftset-statements [user-uri title description draftset-uri created-date]
  (let [ss [draftset-uri
            [rdf:a drafter:DraftSet]
            [drafter:createdAt created-date]
            [drafter:createdBy user-uri]
            [drafter:hasOwner user-uri]]
        ss (util/conj-if (some? title) ss [rdfs:label (s title)])]
    (util/conj-if (some? description) ss [rdfs:comment (s description)])))

(defn create-draftset!
  "Creates a new draftset in the given database and returns its id. If
  no title is provided (i.e. it is nil) a default title will be used
  for the new draftset."
  ([db creator] (create-draftset! db creator nil))
  ([db creator title] (create-draftset! db creator title nil))
  ([db creator title description] (create-draftset! db creator title description (UUID/randomUUID) (Date.)))
  ([db creator title description draftset-id created-date]
   (let [user-uri (user/user->uri creator)
         template (create-draftset-statements user-uri title description (draftset-uri draftset-id) created-date)
         quads (to-quads template)]
     (add db quads)
     (ds/->DraftsetId (str draftset-id)))))

(defn- draftset-exists-query [draftset-ref]
  (str "ASK WHERE {"
       (with-state-graph
         "<" (ds/->draftset-uri draftset-ref) "> a drafter:DraftSet . ")
       "}"))

(defn draftset-exists? [db draftset-ref]
  (let [q (draftset-exists-query draftset-ref)]
    (query db q)))

(defn- delete-statements-for-subject-query [graph-uri subject-uri]
  (str "DELETE { GRAPH <" graph-uri "> { <" subject-uri "> ?p ?o } } WHERE {"
       "  GRAPH <" graph-uri "> { <" subject-uri "> ?p ?o }"
       "}"))

(defn- delete-draftset-statements-query [draftset-ref]
  (let [ds-uri (str (ds/->draftset-uri draftset-ref))]
    (delete-statements-for-subject-query drafter-state-graph ds-uri)))

(defn delete-draftset-statements! [db draftset-ref]
  (let [delete-query (delete-draftset-statements-query draftset-ref)]
    (grafter.rdf.protocols/update! db delete-query)))

(defn- role-scores-values-clause [scored-roles]
  (let [score-pairs (map (fn [[r v]] (format "(\"%s\" %d)" (name r) v)) scored-roles)]
    (clojure.string/join " " score-pairs)))

(defn- get-draftset-graph-mapping-query [draftset-ref]
  (let [ds-uri (str (ds/->draftset-uri draftset-ref))]
    (str
     "SELECT ?lg ?dg WHERE { "
     (with-state-graph
       "<" ds-uri "> a drafter:DraftSet ."
       "?dg drafter:inDraftSet <" ds-uri "> ."
       "?lg a drafter:ManagedGraph ; "
       "    drafter:hasDraft ?dg .")
     "}")))

(defn- get-all-draftset-graph-mappings-query [user]
  (let [username (user/username user)
        role (user/role user)
        user-role-score (role user/role->permission-level)]
    (str
     "SELECT * WHERE { "
     (with-state-graph
       "?ds <"  rdf:a "> <" drafter:DraftSet "> ."
       "?dg <" drafter:inDraftSet "> ?ds ."
       "?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
       "?lg <" drafter:hasDraft "> ?dg ."
       "{"
       "  ?ds <" drafter:hasOwner "> \"" username "\" ."
       "} UNION {"
       "  VALUES (?role ?rv) { " (role-scores-values-clause user/role->permission-level) " }"
       "  ?ds <" drafter:claimableBy "> ?role ."
       "  FILTER ( " user-role-score " >= ?rv )"
       "}")
     "}")))

;;seq {"lg" URI "dg" URI} -> {String String}
(defn- graph-mapping-result-seq->map [mapping-results]
  (into {} (map (fn [{:strs [lg dg]}] [(.stringValue lg) (.stringValue dg)]) mapping-results)))

;;Repository -> String -> Map {String String}
(defn get-draftset-graph-mapping [repo draftset-ref]
  (let [mapping-query (get-draftset-graph-mapping-query draftset-ref)
        results (query repo mapping-query)]
    (graph-mapping-result-seq->map results)))

(defn- graph-mapping-draft-graphs [graph-mapping]
  (vals graph-mapping))

(defn- delete-draftset-query [draftset-ref draft-graph-uris]
  (let [delete-drafts-query (map mgmt/delete-draft-graph-and-remove-from-state-query draft-graph-uris)
        delete-draftset-query (delete-draftset-statements-query draftset-ref)]
    (util/make-compound-sparql-query (conj delete-drafts-query delete-draftset-query))))

(defn delete-draftset!
  "Deletes a draftset and all of its constituent graphs"
  [db draftset-ref]
  (let [graph-mapping (get-draftset-graph-mapping db draftset-ref)
        draft-graphs (graph-mapping-draft-graphs graph-mapping)
        delete-query (delete-draftset-query draftset-ref draft-graphs)]
    (update! db delete-query)))

(defn- graph-mapping-results->map [results]
  (let [draftset-grouped-results (group-by #(get % "ds") results)]
    (into {} (map (fn [[ds-uri mappings]]
                    [(.stringValue ds-uri) (graph-mapping-result-seq->map mappings)])
                  draftset-grouped-results))))

;;Repository -> Map {DraftSetURI -> {String String}}
(defn- get-all-draftset-graph-mappings [repo user]
  (let [results (query repo (get-all-draftset-graph-mappings-query user))]
    (graph-mapping-results->map results)))

(defn- get-draftset-owner-query [draftset-ref]
  (let [draftset-uri (str (ds/->draftset-uri draftset-ref))]
    (str
     "SELECT ?owner WHERE {"
     (with-state-graph
       "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
       "<" draftset-uri "> <" drafter:hasOwner "> ?owner .")
     "}")))

(defn get-draftset-owner [backend draftset-ref]
  (let [q (get-draftset-owner-query draftset-ref)
        result (first (query backend q))
        owner-lit (get result "owner")]
    (and owner-lit (user/uri->username (.stringValue owner-lit)))))

(defn is-draftset-owner? [backend draftset-ref user]
  (let [username (user/username user)
        owner (get-draftset-owner backend draftset-ref)]
    (= owner username)))

(defn- get-draftset-properties-query [draftset-ref]
  (let [draftset-uri (str (ds/->draftset-uri draftset-ref))]
    (str
     "SELECT * WHERE { "
     (with-state-graph
       "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
       "<" draftset-uri "> <" drafter:createdAt "> ?created ."
       "<" draftset-uri "> <" drafter:createdBy "> ?creator ."
       "OPTIONAL { <" draftset-uri "> <" drafter:hasOwner "> ?owner . }"
       "OPTIONAL { <" draftset-uri "> <" rdfs:comment "> ?description . }"
       "OPTIONAL { <" draftset-uri "> <" rdfs:label "> ?title }"
       "OPTIONAL { <" draftset-uri "> <" drafter:claimableBy "> ?role . }"
       "OPTIONAL { <" draftset-uri "> <" drafter:submittedBy "> ?submitter . }")
     "}")))

(defn- get-all-draftsets-properties-query [user]
  (let [user-uri (user/user->uri user)
        role (user/role user)
        user-role-score (role user/role->permission-level)]
    (str
     "SELECT * WHERE { "
     (with-state-graph
       "?ds <" rdf:a "> <" drafter:DraftSet "> ."
       "?ds <" drafter:createdAt "> ?created ."
       "?ds <" drafter:createdBy "> ?creator ."
       "OPTIONAL { ?ds <" rdfs:comment "> ?description . }"
       "OPTIONAL { ?ds <" rdfs:label "> ?title . }"
       "OPTIONAL { ?ds <" drafter:submittedBy "> ?submitter . }"
       "{"
       "  ?ds <" drafter:hasOwner "> <" user-uri "> ."
       "  BIND (<" user-uri "> as ?owner)"
       "} UNION {"
       "  VALUES (?role ?rv) { " (role-scores-values-clause user/role->permission-level) " }"
       "  ?ds <" drafter:claimableBy "> ?role ."
       "  FILTER (" user-role-score " >= ?rv)"
       "}"
       )
     "}")))

(defn- get-all-draftsets-claimable-by-query [user]
  (let [role (user/role user)
        user-uri (user/user->uri user)
        user-role-score (role user/role->permission-level)]
    (str
     "SELECT * WHERE {"
     (with-state-graph
       "?ds <" rdf:a "> <" drafter:DraftSet "> ."
       "?ds <" drafter:createdAt "> ?created ."
       "?ds <" drafter:createdBy "> ?creator ."
       "OPTIONAL { ?ds <" rdfs:comment "> ?description . }"
       "OPTIONAL { ?ds <" rdfs:label "> ?title . }"
       "OPTIONAL { ?ds <" drafter:submittedBy "> ?submitter. }"
       "{"
       "  VALUES (?role ?rv) { " (role-scores-values-clause user/role->permission-level) " }"
       "  ?ds <" drafter:claimableBy "> ?role ."
       "  FILTER (" user-role-score " >= ?rv )"
       "} UNION {"
       "  ?ds <" drafter:submittedBy "> <" user-uri "> ."
       "  ?ds <" drafter:claimableBy "> ?role ."
       "}"
       )
     "}")))

(defn- get-draftsets-claimable-by-graph-mapping-query [user]
  (let [role (user/role user)
        user-role-score (role user/role->permission-level)]
    (str
     "SELECT * WHERE { "
     (with-state-graph
       "VALUES (?role ?rv) { " (role-scores-values-clause user/role->permission-level) " }"
       "?ds <"  rdf:a "> <" drafter:DraftSet "> ."
       "?ds <" drafter:claimableBy "> ?role ."
       "?dg <" drafter:inDraftSet "> ?ds ."
       "?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
       "?lg <" drafter:hasDraft "> ?dg ."
       "FILTER (" user-role-score " >= ?rv )")
     "}")))


(defn- get-draftsets-claimable-by-graph-mapping [backend user]
  (let [q (get-draftsets-claimable-by-graph-mapping-query user)
        results (query backend q)]
    (graph-mapping-results->map results)))

(defn- calendar-literal->date [literal]
  (.. literal (calendarValue) (toGregorianCalendar) (getTime)))

(defn- value->username [val]
  (user/uri->username (.stringValue val)))

(defn- draftset-properties-result->properties [draftset-ref {:strs [created title description creator owner role submitter]}]
  (let [required-fields {:id (str (ds/->draftset-id draftset-ref))
                         :created-at (calendar-literal->date created)
                         :created-by (value->username creator)}
        optional-fields {:display-name (and title (.stringValue title))
                         :description (and description (.stringValue description))
                         :current-owner (and owner (value->username owner))
                         :claim-role (and role (keyword (.stringValue role)))
                         :submitted-by (and submitter (value->username submitter))}]
    (merge required-fields (remove (comp nil? second) optional-fields))))

(defn- get-draftset-properties [repo draftset-ref]
  (let [properties-query (get-draftset-properties-query draftset-ref)
        results (query repo properties-query)]
    (if-let [result (first results)]
      (draftset-properties-result->properties draftset-ref result))))

(defn- combine-draftset-properties-and-graphs [properties graph-mapping]
  (let [live-graph-info (util/map-values (constantly {}) graph-mapping)]
    (assoc properties :changes live-graph-info)))

(defn get-draftset-info [repo draftset-ref]
  (if-let [ds-properties (get-draftset-properties repo draftset-ref)]
    (let [ds-graph-mapping (get-draftset-graph-mapping repo draftset-ref)]
      (combine-draftset-properties-and-graphs ds-properties ds-graph-mapping))))

(defn- combine-all-properties-and-graph-mappings [draftset-properties dataset-graph-mappings]
  (map (fn [{ds-uri "ds" :as result}]
         (let [ds-uri (.stringValue ds-uri)
               properties (draftset-properties-result->properties (ds/->DraftsetURI ds-uri) result)
               graph-mapping (get dataset-graph-mappings ds-uri)]
           (combine-draftset-properties-and-graphs properties graph-mapping)))
       draftset-properties))

(defn get-all-draftsets-info [repo user]
  (let [all-properties (query repo (get-all-draftsets-properties-query user))
        all-graph-mappings (get-all-draftset-graph-mappings repo user)]
    (combine-all-properties-and-graph-mappings all-properties all-graph-mappings)))

(defn is-draftset-submitter? [backend draftset-ref user]
  (if-let [{:keys [submitted-by]} (get-draftset-info backend draftset-ref)]
    (= submitted-by (user/username user))
    false))

(defn get-draftsets-claimable-by [backend user]
  (let [q (get-all-draftsets-claimable-by-query user)
        submitted-properties (query backend q)
        all-graph-mappings (get-draftsets-claimable-by-graph-mapping backend user)]
    (combine-all-properties-and-graph-mappings submitted-properties all-graph-mappings)))

(defn delete-draftset-graph! [db draftset-ref graph-uri]
  (when (mgmt/is-graph-managed? db graph-uri)
    (let [graph-mapping (get-draftset-graph-mapping db draftset-ref)]
      (if-let [draft-graph-uri (get graph-mapping graph-uri)]
        (do
          (mgmt/delete-graph-contents! db draft-graph-uri)
          draft-graph-uri)
        (mgmt/create-draft-graph! db graph-uri {} (str (ds/->draftset-uri draftset-ref)))))))

(def ^:prviate draftset-param->predicate
  {:display-name rdfs:label
   :description rdfs:comment})

(defn- set-draftset-metadata-query [draftset-uri po-pairs]
  (str
   "DELETE {"
   (with-state-graph
     "<" draftset-uri "> ?p ?o .")
   "} INSERT {"
   (with-state-graph
     (string/join " " (map (fn [[p o]] (str "<" draftset-uri "> <" p "> \"" o "\" .")) po-pairs)))
   "} WHERE {"
   (with-state-graph
     "VALUES ?p { " (string/join " " (map #(str "<" (first %) ">") po-pairs)) " }"
     "OPTIONAL { <" draftset-uri "> ?p ?o . }")
   "}"))

(defn set-draftset-metadata!
  "Takes a map containing new values for various metadata keys and
  updates them on the given draftset."
  [backend draftset-ref meta-map]
  (when-let [update-pairs (vals (util/intersection-with draftset-param->predicate meta-map vector))]
    (let [q (set-draftset-metadata-query (ds/->draftset-uri draftset-ref) update-pairs)]
      (update! backend q))))

(defn- submit-draftset-to-role-query [draftset-ref owner role]
  (let [draftset-uri (ds/->draftset-uri draftset-ref)
        user-uri (user/user->uri owner)]
    (str
     "DELETE {"
     (with-state-graph
       "<" draftset-uri "> <" drafter:hasOwner "> <" user-uri "> ."
       "<" draftset-uri "> <" drafter:submittedBy "> ?submitter .")
     "} INSERT {"
     (with-state-graph
       "<" draftset-uri "> <" drafter:claimableBy "> \"" (name role) "\" ."
       "<" draftset-uri "> <" drafter:submittedBy "> <" user-uri "> ."
       )
     "} WHERE {"
     (with-state-graph
       "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
       "<" draftset-uri "> <" drafter:hasOwner "> <" user-uri "> ."
       "OPTIONAL { "
         "<" draftset-uri "> <" drafter:submittedBy "> ?submitter ."
       "}"
       )
     "}")))

(defn submit-draftset-to-role!
  "Submits a draftset to users of the specified role.

  Removes the current owner of a draftset and makes it available to be
  claimed by another user in a particular role. If the given user is
  not the current owner of the draftset, no changes are made."
  [backend draftset-ref owner role]
  (let [q (submit-draftset-to-role-query draftset-ref owner role)]
    (update! backend q)))

(defn- submit-to-user-query [draftset-ref submitter target]
  (let [draftset-uri (str (ds/->draftset-uri draftset-ref))]
    (str
     "DELETE {"
     (with-state-graph
       "<" draftset-uri "> <" drafter:hasOwner "> ?owner ."
       "<" draftset-uri "> <" drafter:claimableBy "> ?claimrole ."
       "<" draftset-uri "> <" drafter:submittedBy "> ?oldsubmitter ."
       )
     "} INSERT {"
     (with-state-graph
       "<" draftset-uri "> <" drafter:hasOwner "> <" (user/user->uri target) "> ."
       "<" draftset-uri "> <" drafter:submittedBy "> <" (user/user->uri submitter) "> ."
       )
     "} WHERE {"
     (with-state-graph
       "OPTIONAL { <" draftset-uri "> <" drafter:hasOwner "> ?owner . }"
       "OPTIONAL { <" draftset-uri "> <" drafter:claimableBy "> ?claimrole . }"
       "OPTIONAL { <" draftset-uri "> <" drafter:submittedBy "> ?oldsubmitter . }")
     "}")))

(defn submit-draftset-to-user! [backend draftset-ref submitter target]
  (let [q (submit-to-user-query draftset-ref submitter target)]
    (update! backend q)))

(defn- try-claim-draftset-query [draftset-ref claimant]
  (let [draftset-uri (ds/->draftset-uri draftset-ref)
        user-uri (user/user->uri claimant)
        role (user/role claimant)
        user-score (user/role->permission-level role)
        scores-values (role-scores-values-clause user/role->permission-level)]
    (str
     "DELETE {"
     (with-state-graph
       "<" draftset-uri "> <" drafter:claimableBy "> ?role .")
     "} INSERT {"
     (with-state-graph
       "<" draftset-uri "> <" drafter:hasOwner "> <" user-uri "> .")
     "} WHERE {"
     "  {"
       (with-state-graph
         "VALUES (?role ?rv) { " scores-values " }"
         "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
         "<" draftset-uri "> <" drafter:claimableBy "> ?role ."
         "FILTER (" user-score " >= ?rv)")
     "  } UNION {"
       (with-state-graph
         "<" draftset-uri "> <" drafter:submittedBy "> <" user-uri "> ."
         "<" draftset-uri "> <" drafter:claimableBy "> ?role .")
     "  }"
     "}")))

(defn- try-claim-draftset!
  "Sets the claiming user to the owner of the given draftset if:
     - the draftset is available (has no current owner)
     - the claiming user is in the appropriate role"
  [backend draftset-ref claimant]
  (let [q (try-claim-draftset-query draftset-ref claimant)]
    (update! backend q)))

(defn- infer-claim-outcome [{:keys [current-owner claim-role] :as ds-info} claimant]
  (if (= (user/username claimant) current-owner)
    :ok
    (cond
     (nil? ds-info) :not-found
     (nil? claim-role) :owned
     (not (user/has-role? claimant claim-role)) :role
     :else :unknown)))

(defn claim-draftset!
  "Attempts to claim a draftset for a user. If the draftset is
  available for claim by the claiming user they will be updated to be
  the new owner. Returns a pair containing the outcome of the
  operation and the current info for the draftset.
  The possible outcomes are:
    - :ok The draftset was claimed by the user
    - :owned Claim failed as the draftset is not available
    - :role Claim failed because the user is not in the claim role
    - :not-found Claim failed because the draftset does not exist
    - :unknown Claim failed for an unknown reason"
  [backend draftset-ref claimant]

  (try-claim-draftset! backend draftset-ref claimant)
  (let [ds-info (get-draftset-info backend draftset-ref)
        outcome (infer-claim-outcome ds-info claimant)]
    [outcome ds-info]))

(defn find-permitted-draftset-operations [backend draftset-ref user]
  (if-let [ds-info (get-draftset-info backend draftset-ref)]
    (user/permitted-draftset-operations ds-info user)
    #{}))

(defn- find-draftset-draft-graph-query [draftset-ref live-graph]
  (let [draftset-uri (str (ds/->draftset-uri draftset-ref))]
    (str
     "SELECT ?dg WHERE {"
     (with-state-graph
       "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
       "?dg <" rdf:a "> <" drafter:DraftGraph "> ."
       "?dg <" drafter:inDraftSet "> <" draftset-uri "> ."
       "<" live-graph "> <" rdf:a "> <" drafter:ManagedGraph "> ."
       "<" live-graph "> <" drafter:hasDraft "> ?dg .")
     "}")))

(defn find-draftset-draft-graph
  "Finds the draft graph for a live graph inside a draftset if one
  exists. Returns nil if the draftset does not exist, or does not
  contain a draft for the graph."
  [backend draftset-ref live-graph]
  (let [q (find-draftset-draft-graph-query draftset-ref live-graph)]
    (when-let [[result] (query backend q)]
      (.stringValue (get result "dg")))))

(defn revert-graph-changes!
  "Reverts the changes made to a live graph inside the given
  draftset. Returns a result indicating the result of the operation:
    - :reverted If the changes were reverted
    - :not-found If the draftset does not exist or no changes exist within it."
  [backend draftset-ref graph]
  (if-let [draft-graph-uri (find-draftset-draft-graph backend draftset-ref graph)]
    (let [ds-uri (ds/->draftset-uri draftset-ref)]
      (mgmt/delete-draft-graph! backend draft-graph-uri)
      :reverted)
    :not-found))

(defn- create-or-empty-draft-graph-for [backend draftset-ref live-graph]
  (if-let [draft-graph-uri (find-draftset-draft-graph backend draftset-ref live-graph)]
    (do
      (mgmt/delete-graph-contents! backend draft-graph-uri)
      draft-graph-uri)
    (mgmt/create-draft-graph! backend live-graph {} (str (ds/->draftset-uri draftset-ref)))))

(defn copy-live-graph-into-draftset-job [backend draftset-ref live-graph]
  (jobs/make-job :batch-write [job]
                 (let [draft-graph-uri (create-or-empty-draft-graph-for backend draftset-ref live-graph)
                       batches (jobs/get-graph-clone-batches backend live-graph)]
                   (jobs/copy-from-live-graph backend live-graph draft-graph-uri batches job))))

(defn- publish-draftset-graphs-joblet [backend draftset-ref]
  (jobs/action-joblet
   (let [graph-mapping (get-draftset-graph-mapping backend draftset-ref)]
     (mgmt/migrate-graphs-to-live! backend (vals graph-mapping)))))

(defn- delete-draftset-joblet [backend draftset-ref]
  (jobs/action-joblet
   (delete-draftset-statements! backend draftset-ref)))

(defn publish-draftset-job
  "Return a job that publishes the graphs in a draftset to live and
  then deletes the draftset."
  [backend draftset-ref]
  (jobs/joblet-seq->job [(publish-draftset-graphs-joblet backend draftset-ref)
                         (delete-draftset-joblet backend draftset-ref)] :batch-write))

(defn quad-batch->graph-triples
  "Extracts the graph-uri from a sequence of quads and converts all
  quads into triples. Expects each quad in the sequence to have the
  same target graph."
  [quads]
  (if-let [graph-uri (context (first quads))]
    {:graph-uri graph-uri :triples (map map->Triple quads)}
    (throw (IllegalArgumentException. "Quad batch must contain at least one item"))))

(defn- append-draftset-quads [backend draftset-ref live->draft quad-batches {:keys [op job-started-at] :as state} job]
  (case op
    :append
    (if-let [batch (first quad-batches)]
      (let [{:keys [graph-uri triples]} (quad-batch->graph-triples batch)]
        (if-let [draft-graph-uri (get live->draft graph-uri)]
          (do
            (mgmt/set-modifed-at-on-draft-graph! backend draft-graph-uri job-started-at)
            (mgmt/append-data-batch! backend draft-graph-uri triples)
            (let [next-job (create-child-job
                            job
                            (partial append-draftset-quads backend draftset-ref live->draft (rest quad-batches) (merge state {:op :append})))]
              (scheduler/queue-job! next-job)))
          ;;NOTE: do this immediately instead of scheduling a
          ;;continuation since we haven't done any real work yet
          (append-draftset-quads backend draftset-ref live->draft quad-batches (merge state {:op :copy-graph :graph graph-uri}) job)))
      (jobs/job-succeeded! job))

    :copy-graph
    (let [live-graph-uri (:graph state)
          ds-uri (str (ds/->draftset-uri draftset-ref))
          {:keys [draft-graph-uri graph-map]} (mgmt/ensure-draft-exists-for backend live-graph-uri live->draft ds-uri)
          clone-batches (jobs/get-graph-clone-batches backend live-graph-uri)
          copy-batches-state (merge state {:op :copy-graph-batches
                                           :graph live-graph-uri
                                           :draft-graph draft-graph-uri
                                           :batches clone-batches})]
      ;;NOTE: do this immediately since we still haven't done any real work yet...
      (append-draftset-quads backend draftset-ref graph-map quad-batches copy-batches-state job))

    :copy-graph-batches
    (let [{:keys [graph batches draft-graph]} state]
      (if-let [[offset limit] (first batches)]
        (do
          (jobs/copy-graph-batch! backend graph draft-graph offset limit)
          (let [next-state (update-in state [:batches] rest)
                next-job (create-child-job
                          job
                          (partial append-draftset-quads backend draftset-ref live->draft quad-batches next-state))]
            (scheduler/queue-job! next-job)))
        ;;graph copy completed so continue appending quads
        ;;NOTE: do this immediately since we haven't done any work on this iteration
        (append-draftset-quads backend draftset-ref live->draft quad-batches (merge state {:op :append}) job)))))

(defn- append-quads-to-draftset-job [backend draftset-ref quads]
  (let [graph-map (get-draftset-graph-mapping backend draftset-ref)
        quad-batches (util/batch-partition-by quads context jobs/batched-write-size)
        now (java.util.Date.)
        append-data (partial append-draftset-quads backend draftset-ref graph-map quad-batches {:op :append
                                                                                                :job-started-at now })]
    (create-job :batch-write append-data)))

(defn append-data-to-draftset-job [backend draftset-ref tempfile rdf-format]
  (append-quads-to-draftset-job backend draftset-ref (read-statements tempfile rdf-format)))

(defn append-triples-to-draftset-job [backend draftset-ref tempfile rdf-format graph]
  (let [triples (read-statements tempfile rdf-format)
        quads (map (comp map->Quad #(assoc % :c graph)) triples)]
    (append-quads-to-draftset-job backend draftset-ref quads)))
