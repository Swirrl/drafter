(ns drafter.backend.draftset.operations
  (:require [clojure.string :as string]
            [drafter.backend.draftset.draft-management
             :as
             mgmt
             :refer
             [to-quads with-state-graph]]
            [drafter.backend.draftset.rewrite-result :refer [rewrite-statement]]
            [drafter.draftset :as ds]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sesame :refer [read-statements]]
            [drafter.rdf.sparql :as sparql]
            [drafter.user :as user]
            [drafter.util :as util]
            [drafter.write-scheduler :as writes]
            [grafter-2.rdf.protocols :as rdf :refer [map->Quad map->Triple context]]
            [grafter-2.rdf4j.repository :refer [prepare-query]]
            [grafter-2.rdf4j.formats :as formats]
            [grafter-2.rdf4j.io :refer [quad->backend-quad rdf-writer]]
            [grafter-2.rdf4j.repository :as repo]
            [grafter.url :as url]
            [grafter.vocabularies.rdf :refer :all]
            [swirrl-server.async.jobs :as ajobs])
  (:import java.io.StringWriter
           [java.util Date]
           org.eclipse.rdf4j.model.impl.ContextStatementImpl
           org.eclipse.rdf4j.model.Resource
           [org.eclipse.rdf4j.query GraphQuery TupleQueryResultHandler TupleQueryResult]
           [org.eclipse.rdf4j.rio RDFHandler RDFWriter]
           org.eclipse.rdf4j.queryrender.RenderUtils))

(defn- create-draftset-statements [user-uri title description draftset-uri created-date]
  (let [ss [draftset-uri
            [rdf:a drafter:DraftSet]
            [drafter:createdAt created-date]
            [drafter:modifiedAt created-date]
            [drafter:createdBy user-uri]
            [drafter:hasOwner user-uri]]
        ss (util/conj-if (some? title) ss [rdfs:label title])]
    (util/conj-if (some? description) ss [rdfs:comment description])))

(defn create-draftset!
  "Creates a new draftset in the given database and returns its id. If
  no title is provided (i.e. it is nil) a default title will be used
  for the new draftset."
  ([db creator] (create-draftset! db creator nil))
  ([db creator title] (create-draftset! db creator title nil))
  ([db creator title description] (create-draftset! db creator title description util/create-uuid util/get-current-time))
  ([db creator title description id-creator created-date-fn]
   (with-open [dbcon (repo/->connection db)]
     (let [draftset-id (id-creator)
           created-date (created-date-fn)
           user-uri (user/user->uri creator)
           template (create-draftset-statements user-uri title description (url/append-path-segments draftset-uri draftset-id) created-date)
           quads (to-quads template)]
       (rdf/add dbcon quads)
       (ds/->DraftsetId (str draftset-id))))))

(defn- graph-exists-query [graph-uri]
  (str
   "ASK WHERE {"
   "  GRAPH <" graph-uri "> { ?s ?p ?o }"
   "}"))

(defn- draftset-exists-query [draftset-ref]
  (str "ASK WHERE {"
       (with-state-graph
         "<" (ds/->draftset-uri draftset-ref) "> a <" drafter:DraftSet "> .")
       "}"))

(defn draftset-exists? [db draftset-ref]
  (let [q (draftset-exists-query draftset-ref)]
    (sparql/eager-query db q)))

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
    (sparql/update! db delete-query)))

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
     "} LIMIT 1")))

(defn get-draftset-owner [backend draftset-ref]
  (let [q (get-draftset-owner-query draftset-ref)
        result (first (sparql/eager-query backend q))]
    (when-let [owner-uri (:owner result)]
      (user/uri->username owner-uri))))

(defn is-draftset-owner? [backend draftset-ref user]
  (let [username (user/username user)
        owner (get-draftset-owner backend draftset-ref)]
    (= owner username)))

(defn- get-draftset-graph-status [{:keys [public draft-graph-exists]}]
  {:pre [(some? draft-graph-exists)]}
  (cond (and public draft-graph-exists) :updated
        (and public (not draft-graph-exists)) :deleted
        (and (not public) draft-graph-exists) :created
        :else :deleted))

(defn- graph-states->changes-map [states]
  (into {} (map (fn [{:keys [live-graph-uri] :as state}]
                  [live-graph-uri {:status (get-draftset-graph-status state)}])
                 states)))

(defn- graph-mapping-result->graph-mapping [{:keys [lg dg]}]
  {:live-graph-uri lg
   :draft-graph-uri dg})

(defn- graph-mapping-result->graph-state [repo {:keys [lg dg public] :as res}]
  (assoc (graph-mapping-result->graph-mapping res)
         :public public
         :draft-graph-exists (sparql/eager-query repo (graph-exists-query dg))))

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
      "?ds <" drafter:modifiedAt "> ?modified ."
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
      "  " (union-clauses match-clauses)
      "  }"
      "}")
    "}"))

(defn- draftset-uri-clause [draftset-ref]
  (str
    "{ VALUES ?ds { <" (str (ds/->draftset-uri draftset-ref)) "> } }"))

(defn get-draftset-graph-mapping-query [draftset-ref]
  (get-draftsets-matching-graph-mappings-query
    [(draftset-uri-clause draftset-ref)]))

(defn get-draftset-graph-states [repo draftset-ref]
  (let [q (get-draftset-graph-mapping-query draftset-ref)]
    (->> q
         (sparql/eager-query repo)
         (map graph-mapping-result->graph-mapping))))

(defn get-draftset-graph-mapping [repo draftset-ref]
  (let [graph-states (get-draftset-graph-states repo draftset-ref)
        mapping-pairs (map (juxt :live-graph-uri :draft-graph-uri) graph-states)]
    (into {} mapping-pairs)))

(defn- user-is-owner-clause [user]
  (str "{ ?ds <" drafter:hasOwner "> <" (user/user->uri user) "> . }"))

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

(defn- draftset-properties-result->properties [draftset-ref {:keys [created title description creator owner role claimuser submitter modified] :as ds}]
  (let [required-fields {:id (str (ds/->draftset-id draftset-ref))
                         :created-at created
                         :created-by (user/uri->username creator)
                         :updated-at modified}
        optional-fields {:display-name title
                         :description description
                         :current-owner (some-> owner (user/uri->username))
                         :claim-role (some-> role (keyword))
                         :claim-user (some-> claimuser (user/uri->username))
                         :submitted-by (some-> submitter (user/uri->username))}]
    (merge required-fields (remove (comp nil? second) optional-fields))))

(defn- combine-draftset-properties-and-graph-states [ds-properties graph-states]
  (assoc ds-properties :changes (graph-states->changes-map graph-states)))


(defn- combine-all-properties-and-graph-states [draftset-properties graph-states]
  (let [ds-uri->graph-states (group-by :draftset-uri graph-states)]
    (map (fn [{ds-uri :ds :as result}]
           (let [properties (draftset-properties-result->properties (ds/->DraftsetURI ds-uri) result)
                 ds-graph-states (get ds-uri->graph-states ds-uri)]
             (combine-draftset-properties-and-graph-states properties ds-graph-states)))
         draftset-properties)))

(defn- draftset-graph-mappings->graph-states [repo mappings]
  (map (fn [{:keys [ds] :as m}]
           (let [graph-state (graph-mapping-result->graph-state repo m)]
             (assoc graph-state :draftset-uri ds)))
         mappings))

(defn- get-all-draftsets-properties-by [repo clauses]
  (let [properties-query (get-draftsets-matching-properties-query clauses)]
    (sparql/eager-query repo properties-query)))

(defn- get-all-draftsets-mappings-by [repo clauses]
  (let [mappings-query (get-draftsets-matching-graph-mappings-query clauses)]
    (sparql/eager-query repo mappings-query)))

(defn get-all-draftsets-by [repo clauses]
  (let [properties (get-all-draftsets-properties-by repo clauses)
        graph-mappings (get-all-draftsets-mappings-by repo clauses)
        graph-states (draftset-graph-mappings->graph-states repo graph-mappings)]

    (combine-all-properties-and-graph-states properties graph-states)))

(defn get-draftset-info [repo draftset-ref]
  (first (get-all-draftsets-by repo [(draftset-uri-clause draftset-ref)])))

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
    (sparql/update! db delete-query)))

(defn delete-draftset-graph!
  "Mark the graph for deletion in live by removing its contents.  If
  the graph doesn't exist in the draftset we create an empty draft
  graph for it.  Publishing the empty graph will then result in a
  deletion from live.

  modified-time-fn - A 0-arg function that returns the modified-time."
  [db draftset-ref graph-uri modified-time-fn]
  (when (mgmt/is-graph-managed? db graph-uri)
    (let [graph-mapping (get-draftset-graph-mapping db draftset-ref)]
      (if-let [draft-graph-uri (get graph-mapping graph-uri)]
        (let [modified-at (modified-time-fn)]
          (mgmt/delete-graph-contents! db draft-graph-uri modified-at)
          draft-graph-uri)
        (mgmt/create-draft-graph! db graph-uri draftset-ref modified-time-fn)))))

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
      (sparql/update! backend q))))

(defn- submit-draftset-to-role-query [draftset-ref submission-id owner role]
  (let [draftset-uri (ds/->draftset-uri draftset-ref)
        submit-uri (submission-id->uri submission-id)
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
  (let [q (submit-draftset-to-role-query draftset-ref (util/create-uuid) owner role)]
    (sparql/update! backend q)))

(defn- submit-to-user-query [draftset-ref submission-id submitter target]
  (let [submitter-uri (user/user->uri submitter)
        target-uri (user/user->uri target)
        submit-uri (submission-id->uri submission-id)
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
  (let [q (submit-to-user-query draftset-ref (util/create-uuid) submitter target)]
    (sparql/update! backend q)))

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
    (sparql/update! backend q)))

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
     "} LIMIT 1")))

(defn find-draftset-draft-graph
  "Finds the draft graph for a live graph inside a draftset if one
  exists. Returns nil if the draftset does not exist, or does not
  contain a draft for the graph."
  [backend draftset-ref live-graph]
  (let [q (find-draftset-draft-graph-query draftset-ref live-graph)
        [result] (sparql/eager-query backend q)]
    (:dg result)))

(defn publish-draftset-graphs! [backend draftset-ref clock-fn]
  (let [graph-mapping (get-draftset-graph-mapping backend draftset-ref)]
    (mgmt/migrate-graphs-to-live! backend (vals graph-mapping) clock-fn)))

(defn- rdf-handler->spog-tuple-handler [conn ^RDFHandler rdf-handler]
  (reify
    TupleQueryResult
    (getBindingNames [this]
      ;; hard coded as part of this test stub
      ["s" "p" "o" "g"])

    TupleQueryResultHandler
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
      (.endRDF rdf-handler)
      (.close conn))))

(defn- spog-tuple-query->graph-query [conn tuple-query]
  (reify GraphQuery
    (evaluate [this rdf-handler]
      (.evaluate tuple-query (rdf-handler->spog-tuple-handler conn rdf-handler)))
    (getIncludeInferred [this]
      (.getIncludeInferred tuple-query))
    (setIncludeInferred [this include-inferred?]
      (.setIncludeInferred tuple-query include-inferred?))
    (getMaxExecutionTime [this]
      (.getMaxExecutionTime tuple-query))
    (setMaxExecutionTime [this max]
      (.setMaxExecutionTime tuple-query max))))

(defn all-quads-query
  "Returns a Sesame GraphQuery for all quads in the draftset
  represented by the given backend."
  [backend]
  (let [conn (repo/->connection backend)
        tuple-query (repo/prepare-query conn "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }")]
    (spog-tuple-query->graph-query conn tuple-query)))

(defn all-graph-triples-query [backend graph]
  (let [conn (repo/->connection backend)
        unsafe-query (format "CONSTRUCT {?s ?p ?o} WHERE { GRAPH <%s> { ?s ?p ?o } }" graph)
        escaped-query (RenderUtils/escape unsafe-query)]
    (prepare-query conn escaped-query)))
