(ns drafter.rdf.draftset-management
  (:require [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [add s context]]
            [grafter.rdf.io :refer [IStatement->sesame-statement]]
            [grafter.rdf.protocols :refer [map->Triple map->Quad]]
            [grafter.rdf.repository :as repo]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.sesame :refer [read-statements]]
            [drafter.rdf.rewriting.result-rewriting :refer [rewrite-query-results rewrite-statement]]
            [drafter.backend.protocols :refer [prepare-query ->sesame-repo]]
            [drafter.util :as util]
            [drafter.draftset :as ds]
            [drafter.user :as user]
            [drafter.util :as util]
            [drafter.rdf.draft-management :refer [update! query to-quads with-state-graph drafter-state-graph] :as mgmt]
            [drafter.rdf.draft-management.jobs :as jobs]
            [swirrl-server.async.jobs :refer [create-job create-child-job job-succeeded!]]
            [drafter.write-scheduler :as scheduler]
            [schema.core :as s]
            [clojure.string :as string])
  (:import [java.util Date UUID]
           [org.openrdf.model Resource URI]
           [org.openrdf.model.impl ContextStatementImpl]
           [org.openrdf.query TupleQueryResultHandler GraphQuery]))

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

(defn- graph-exists-query [graph-uri]
  (str
   "ASK WHERE {"
   "  GRAPH <" graph-uri "> { ?s ?p ?o }"
   "}"))

(defn- draftset-exists-query [draftset-ref]
  (str "ASK WHERE {"
       (with-state-graph
         "<" (ds/->draftset-uri draftset-ref) "> a drafter:DraftSet . ")
       "}"))

(defn draftset-exists? [db draftset-ref]
  (let [q (draftset-exists-query draftset-ref)]
    (query db q)))

(defn- delete-draftset-statements-query [draftset-ref]
  (let [ds-uri (str (ds/->draftset-uri draftset-ref))]
    (str
     "DELETE {"
     (with-state-graph
       "<" ds-uri  "> ?dp ?do ."
       "?submission ?sp ?so .")
     "} WHERE {"
     (with-state-graph
       "<" ds-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
       "<" ds-uri "> ?dp ?do ."
       "OPTIONAL {"
       "  <" ds-uri "> <" drafter:hasSubmission "> ?submission ."
       "  ?submission ?sp ?so ."
       "}"
       )
     "}")))

(defn delete-draftset-statements! [db draftset-ref]
  (let [delete-query (delete-draftset-statements-query draftset-ref)]
    (update! db delete-query)))

(defn- role-scores-values-clause [scored-roles]
  (let [score-pairs (map (fn [[r v]] (format "(\"%s\" %d)" (name r) v)) scored-roles)]
    (clojure.string/join " " score-pairs)))

(defn- graph-mapping-draft-graphs [graph-mapping]
  (vals graph-mapping))

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

(defn- get-draftset-graph-status [{:keys [public draft-graph-exists]}]
  (cond (and public draft-graph-exists) :updated
        (and public (not draft-graph-exists)) :deleted
        (and (not public) draft-graph-exists) :created
        :else :deleted))

(defn- graph-states->changes-map [states]
  (into {} (map (fn [{:keys [live-graph-uri] :as state}]
                  [live-graph-uri {:status (get-draftset-graph-status state)}])
                 states)))

(defn- graph-mapping-result->graph-state [repo {:strs [lg dg public]}]
  {:live-graph-uri (.stringValue lg)
   :draft-graph-uri (.stringValue dg)
   :public (.booleanValue public)
   :draft-graph-exists (.booleanValue (query repo (graph-exists-query dg)))})

(defn- union-clauses [clauses]
  (string/join " UNION " clauses))

(defn- get-draftsets-matching-graph-mappings-query [match-clauses]
  (str
    "SELECT * WHERE { "
    (with-state-graph
      "?ds <"  rdf:a "> <" drafter:DraftSet "> ."
      "?dg <" drafter:inDraftSet "> ?ds ."
      "?lg <" rdf:a "> <" drafter:ManagedGraph "> ."
      "?lg <" drafter:hasDraft "> ?dg ."
      "?lg <" drafter:isPublic "> ?public ."
      "{"
      "  SELECT DISTINCT ?ds WHERE {"
      "  ?ds <" rdf:a "> <" drafter:DraftSet "> ."
      (union-clauses match-clauses)
      "  }"
      "}")
    "}"))

(defn- get-draftsets-matching-properties-query [match-clauses]
  (str
    "SELECT * WHERE {"
    (with-state-graph
      "?ds <" drafter:createdAt "> ?created ."
      "?ds <" drafter:createdBy "> ?creator ."
      "OPTIONAL { ?ds <" rdfs:comment "> ?description . }"
      "OPTIONAL { ?ds <" drafter:hasOwner "> ?owner . }"
      "OPTIONAL { ?ds <" rdfs:label "> ?title . }"
      "OPTIONAL { ?ds <" drafter:submittedBy "> ?submitter. }"
      "OPTIONAL {"
      "  ?ds <" drafter:hasSubmission "> ?submission ."
      "  ?submission <" drafter:claimUser "> ?claimuser ."
      "}"
      "OPTIONAL {"
      "  ?ds <" drafter:hasSubmission "> ?submission ."
      "  ?submission <" drafter:claimRole "> ?role ."
      "}"
      "{"
      "  SELECT DISTINCT ?ds WHERE {"
      "  ?ds <" rdf:a "> <" drafter:DraftSet "> ."
      (union-clauses match-clauses)
      "  }"
      "}")
    "}"))

(defn- draftset-uri-clause [draftset-ref]
  (str
    "{ VALUES ?ds { <" (str (ds/->draftset-uri draftset-ref)) "> } }"))

(defn- get-draftset-properties-query [draftset-ref]
  (get-draftsets-matching-properties-query
    [(draftset-uri-clause draftset-ref)]))

(defn- get-draftset-graph-mapping-query [draftset-ref]
  (get-draftsets-matching-graph-mappings-query
    [(draftset-uri-clause draftset-ref)]))

(defn get-draftset-graph-states [repo draftset-ref]
  (->> (get-draftset-graph-mapping-query draftset-ref)
       (query repo)
       (map #(graph-mapping-result->graph-state repo %))))

(defn get-draftset-graph-mapping [repo draftset-ref]
  (let [graph-states (get-draftset-graph-states repo draftset-ref)
        mapping-pairs (map #(mapv % [:live-graph-uri :draft-graph-uri]) graph-states)]
    (into {} mapping-pairs)))

(defn- user-is-owner-clause [user]
  (str "{ ?ds <" drafter:hasOwner "> <" (user/user->uri user) "> . }"))

(defn- user-is-claim-user-clause [user]
  (str
    "{"
    "  ?ds <" drafter:hasSubmission "> ?submission ."
    "  ?submission <" drafter:claimUser "> <" (user/user->uri user) "> ."
    "}"))

(defn- user-is-in-claim-role-clause [user]
  (let [role (user/role user)
        user-role-score (role user/role->permission-level)]
    (str
      "{"
      "   VALUES (?role ?rv) { " (role-scores-values-clause user/role->permission-level) " }"
      "  ?ds <" drafter:hasSubmission "> ?submission ."
      "  ?submission <" drafter:claimRole "> ?role ."
      "  FILTER (" user-role-score " >= ?rv)"
      "}")))

(defn- user-is-submitter-clause [user]
  (str
    "{"
    "  ?ds <" drafter:submittedBy "> <" (user/user->uri user) "> ."
    "  FILTER NOT EXISTS { ?ds <" drafter:hasOwner "> ?owner }"
    "}")
  )

(defn- user-claimable-clauses [user]
  [(user-is-in-claim-role-clause user)
   (user-is-claim-user-clause user)
   (user-is-submitter-clause user)])

(defn- user-all-visible-clauses [user]
  (conj (user-claimable-clauses user)
        (user-is-owner-clause user)))

(defn- calendar-literal->date [literal]
  (.. literal (calendarValue) (toGregorianCalendar) (getTime)))

(defn- value->username [val]
  (user/uri->username (.stringValue val)))

(defn- draftset-properties-result->properties [draftset-ref {:strs [created title description creator owner role claimuser submitter]}]
  (let [required-fields {:id (str (ds/->draftset-id draftset-ref))
                         :created-at (calendar-literal->date created)
                         :created-by (value->username creator)}
        optional-fields {:display-name (and title (.stringValue title))
                         :description (and description (.stringValue description))
                         :current-owner (and owner (value->username owner))
                         :claim-role (and role (keyword (.stringValue role)))
                         :claim-user (and claimuser (value->username claimuser))
                         :submitted-by (and submitter (value->username submitter))}]
    (merge required-fields (remove (comp nil? second) optional-fields))))

(defn- combine-draftset-properties-and-graph-states [ds-properties graph-states]
  (assoc ds-properties :changes (graph-states->changes-map graph-states)))


(defn- combine-all-properties-and-graph-states [draftset-properties graph-states]
  (let [ds-uri->graph-states (group-by :draftset-uri graph-states)]
    (map (fn [{ds-uri "ds" :as result}]
           (let [ds-uri (.stringValue ds-uri)
                 properties (draftset-properties-result->properties (ds/->DraftsetURI ds-uri) result)
                 ds-graph-states (get ds-uri->graph-states ds-uri)]
             (combine-draftset-properties-and-graph-states properties ds-graph-states)))
         draftset-properties)))

(defn- draftset-graph-mappings->graph-states [repo mappings]
  (map (fn [{:strs [ds] :as m}]
           (let [graph-state (graph-mapping-result->graph-state repo m)]
             (assoc graph-state :draftset-uri (.stringValue ds))))
         mappings))

(defn- get-all-draftsets-by [repo clauses]
  (let [properties-query (get-draftsets-matching-properties-query clauses)
        mappings-query (get-draftsets-matching-graph-mappings-query clauses)
        properties (query repo properties-query)
        graph-mappings (query repo mappings-query)
        graph-states (draftset-graph-mappings->graph-states repo graph-mappings)]
    (combine-all-properties-and-graph-states properties graph-states)))

(defn get-draftset-info [repo draftset-ref]
  (first (get-all-draftsets-by repo [(draftset-uri-clause draftset-ref)])))

(defn get-all-draftsets-info [repo user]
  (get-all-draftsets-by repo (user-all-visible-clauses user)))

(defn get-draftsets-claimable-by [repo user]
  (get-all-draftsets-by repo (user-claimable-clauses user)))

(defn get-draftsets-owned-by [repo user]
  (get-all-draftsets-by repo [(user-is-owner-clause user)]))

(defn is-draftset-submitter? [backend draftset-ref user]
  (if-let [{:keys [submitted-by]} (get-draftset-info backend draftset-ref)]
    (= submitted-by (user/username user))
    false))

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

(defn delete-draftset-graph! [db draftset-ref graph-uri]
  (when (mgmt/is-graph-managed? db graph-uri)
    (let [graph-mapping (get-draftset-graph-mapping db draftset-ref)]
      (if-let [draft-graph-uri (get graph-mapping graph-uri)]
        (do
          (mgmt/delete-graph-contents! db draft-graph-uri)
          draft-graph-uri)
        (mgmt/create-draft-graph! db graph-uri {} (str (ds/->draftset-uri draftset-ref)))))))

(def ^:private draftset-param->predicate
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

(defn- submit-draftset-to-role-query [draftset-ref submission-id owner role]
  (let [draftset-uri (ds/->draftset-uri draftset-ref)
        submit-uri (submission-uri submission-id)
        user-uri (user/user->uri owner)]
    (str
     "DELETE {"
     (with-state-graph
       "<" draftset-uri "> <" drafter:hasOwner "> <" user-uri "> ."
       "<" draftset-uri "> <" drafter:submittedBy "> ?submitter .")
     "} INSERT {"
     (with-state-graph
       "<" submit-uri "> <" rdf:a "> <" drafter:Submission "> ."
       "<" submit-uri "> <" drafter:claimRole "> \"" (name role) "\" ."
       "<" draftset-uri "> <" drafter:hasSubmission "> <" submit-uri "> ."
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
  (let [q (submit-draftset-to-role-query draftset-ref (UUID/randomUUID) owner role)]
    (update! backend q)))

(defn- submit-to-user-query [draftset-ref submission-id submitter target]
  (let [submitter-uri (user/user->uri submitter)
        target-uri (user/user->uri target)
        submit-uri (submission-uri submission-id)
        draftset-uri (str (ds/->draftset-uri draftset-ref))]
    (str
     "DELETE {"
     (with-state-graph
       "<" draftset-uri "> <" drafter:hasOwner "> <" submitter-uri "> ."
       "<" draftset-uri "> <" drafter:submittedBy "> ?submitter .")
     "} INSERT {"
     (with-state-graph
       "<" submit-uri "> <" rdf:a "> <" drafter:Submission "> ."
       "<" submit-uri "> <" drafter:claimUser "> <" target-uri "> ."
       "<" draftset-uri "> <" drafter:hasSubmission "> <" submit-uri "> ."
       "<" draftset-uri "> <" drafter:submittedBy "> <" submitter-uri "> ."
       )
     "} WHERE {"
     (with-state-graph
       "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
       "<" draftset-uri "> <" drafter:hasOwner "> <" submitter-uri "> ."
       "OPTIONAL { "
         "<" draftset-uri "> <" drafter:submittedBy "> ?submitter ."
       "}"
       )
     "}")))

(defn submit-draftset-to-user! [backend draftset-ref submitter target]
  (let [q (submit-to-user-query draftset-ref (UUID/randomUUID) submitter target)]
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
       "<" draftset-uri "> <" drafter:hasSubmission "> ?submission ."
       "?submission ?sp ?so .")
     "} INSERT {"
     (with-state-graph
       "<" draftset-uri "> <" drafter:hasOwner "> <" user-uri "> .")
     "} WHERE {"
     (with-state-graph
       "<" draftset-uri "> <" rdf:a "> <" drafter:DraftSet "> ."
       "<" draftset-uri "> <" drafter:hasSubmission "> ?submission ."
       "?submission ?sp ?so ."
       "{"
       "  SELECT DISTINCT ?submission WHERE {"
       "    {"
       "       VALUES (?role ?rv) { " scores-values " }"
       "       ?submission <" drafter:claimRole "> ?role ."
       "       FILTER (" user-score " >= ?rv)"
       "    } UNION {"
       "      ?submission <" drafter:claimUser "> <" user-uri "> ."
       "    } UNION {"
       "      <" draftset-uri "> <" drafter:submittedBy "> <" user-uri "> ."
       "      <" draftset-uri "> <" drafter:hasSubmission "> ?submission ."
       "    }"
       "  }"
       "}")
     "}")))

(defn- try-claim-draftset!
  "Sets the claiming user to the owner of the given draftset if:
     - the draftset is available (has no current owner)
     - the claiming user is in the appropriate role"
  [backend draftset-ref claimant]
  (let [q (try-claim-draftset-query draftset-ref claimant)]
    (update! backend q)))

(defn- infer-claim-outcome [{:keys [current-owner claim-role claim-user] :as ds-info} claimant] 
  (cond
    (nil? ds-info) :not-found
    (= (user/username claimant) current-owner) :ok
    (some? current-owner) :owned
    (and (some? claim-role)
         (not (user/has-role? claimant claim-role))) :role
    (and (some? claim-user)
         (not= claim-user (user/username claimant))) :user
    :else :unknown))

(defn claim-draftset!
  "Attempts to claim a draftset for a user. If the draftset is
  available for claim by the claiming user they will be updated to be
  the new owner. Returns a pair containing the outcome of the
  operation and the current info for the draftset.
  The possible outcomes are:
    - :ok The draftset was claimed by the user
    - :owned Claim failed as the draftset is not available
    - :role Claim failed because the user is not in the claim role
    - :user Claim failed because the user is not the assigned user
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
      (job-succeeded! job))

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

(defn- delete-quads-from-draftset [backend quad-batches draftset-ref live->draft {:keys [op job-started-at] :as state} job]
  (case op
    :delete
    (if-let [batch (first quad-batches)]
      (let [live-graph (context (first batch))]
        (if (mgmt/is-graph-managed? backend live-graph)
          (if-let [draft-graph-uri (get live->draft live-graph)]
            (do
              (mgmt/set-modifed-at-on-draft-graph! backend draft-graph-uri job-started-at)
              (with-open [conn (repo/->connection (->sesame-repo backend))]
                (let [rewritten-statements (map #(rewrite-statement live->draft %) batch)
                      sesame-statements (map IStatement->sesame-statement rewritten-statements)
                      graph-array (into-array Resource (map util/string->sesame-uri (vals live->draft)))]
                  (.remove conn sesame-statements graph-array)))
              (let [next-job (create-child-job
                              job
                              (partial delete-quads-from-draftset backend (rest quad-batches) draftset-ref live->draft state))]
                (scheduler/queue-job! next-job)))
            ;;NOTE: Do this immediately as we haven't done any real work yet
            (recur backend quad-batches draftset-ref live->draft (merge state {:op :copy-graph :live-graph live-graph}) job))
          ;;live graph does not exist so do not create a draft graph
          ;;NOTE: This is the same behaviour as deleting a live graph
          ;;which does not exist in live
          (recur backend (rest quad-batches) draftset-ref live->draft state job)))
      (let [draftset-info (get-draftset-info backend draftset-ref)]
        (job-succeeded! job {:draftset draftset-info})))

    :copy-graph
    (let [{:keys [live-graph]} state
          draft-graph-uri (mgmt/create-draft-graph! backend live-graph {} (str (ds/->draftset-uri draftset-ref)))
          copy-batches (jobs/get-graph-clone-batches backend live-graph)
          copy-state {:op :copy-graph-batches
                      :graph live-graph
                      :draft-graph-uri draft-graph-uri
                      :batches copy-batches}]
      ;;NOTE: Do this immediately as no work has been done yet
      (recur backend quad-batches draftset-ref (assoc live->draft live-graph draft-graph-uri) (merge state copy-state) job))

    :copy-graph-batches
    (let [{:keys [graph batches draft-graph-uri]} state]
      (if-let [[offset limit] (first batches)]
        (do
          (jobs/copy-graph-batch! backend graph draft-graph-uri offset limit)
          (let [next-state (update-in state [:batches] rest)
                next-job (create-child-job
                          job
                          (partial delete-quads-from-draftset backend quad-batches draftset-ref live->draft (merge state next-state)))]
            (scheduler/queue-job! next-job)))
        ;;graph copy completed so continue deleting quads
        ;;NOTE: do this immediately since we haven't done any work on this iteration
        (recur backend quad-batches draftset-ref live->draft (merge state {:op :delete}) job)))))

(defn- batch-and-delete-quads-from-draftset [backend quads draftset-ref live->draft job]
  (let [quad-batches (util/batch-partition-by quads context jobs/batched-write-size)
        now (java.util.Date.)]
    (delete-quads-from-draftset backend quad-batches draftset-ref live->draft {:op :delete :job-started-at now} job)))

(defn- stringified-draftset-backend-graph-mapping
  "Gets a map of type {String String} containing the live->draft graph
  mapping of a RewritingSesameSparqlExecutor. WARNING: This is coupled
  to the implementation of RewritingSesameSparqlExecutor since it
  relies on the name of the graph mapping field."
  [ds-backend]
  (let [graph-mapping (:live->draft ds-backend)]
    (util/map-all #(.stringValue %) graph-mapping)))

(defn delete-quads-from-draftset-job [backend draftset-ref serialised rdf-format]
  (jobs/make-job
   :batch-write [job]
   (let [quads (read-statements serialised rdf-format)
         graph-mapping (stringified-draftset-backend-graph-mapping backend)]
     (batch-and-delete-quads-from-draftset backend quads draftset-ref graph-mapping job))))

(defn delete-triples-from-draftset-job [backend draftset-ref graph serialised rdf-format]
  (jobs/make-job
     :batch-write [job]
     (let [triples (read-statements serialised rdf-format)
           quads (map #(util/make-quad-statement % graph) triples)
           graph-mapping (stringified-draftset-backend-graph-mapping backend)]
       (batch-and-delete-quads-from-draftset backend quads draftset-ref graph-mapping job))))

(defn- rdf-handler->spog-tuple-handler [rdf-handler]
  (reify TupleQueryResultHandler
    (handleSolution [this bindings]
      (let [subj (.getValue bindings "s")
            pred (.getValue bindings "p")
            obj (.getValue bindings "o")
            graph (.getValue bindings "g")
            stmt (ContextStatementImpl. subj pred obj graph)]
        (.handleStatement rdf-handler stmt)))

    (handleBoolean [this b])
    (handleLinks [this links])
    (startQueryResult [this binding-names]
      (.startRDF rdf-handler))
    (endQueryResult [this]
      (.endRDF rdf-handler))))

(defn- spog-tuple-query->graph-query [tuple-query]
  (reify GraphQuery
    (evaluate [this rdf-handler]
      (.evaluate tuple-query (rdf-handler->spog-tuple-handler rdf-handler)))))

(defn all-quads-query
  "Returns a Sesame GraphQuery for all quads in the draftset
  represented by the given backend."
  [backend]
  (let [tuple-query (prepare-query backend "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }")]
    (spog-tuple-query->graph-query tuple-query)))
