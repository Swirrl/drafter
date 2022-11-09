(ns drafter.backend.draftset.operations
  (:require [clojure.string :as string]
            [com.yetanalytics.flint :as fl]
            [drafter.backend.draftset.draft-management :as mgmt :refer [to-quads]]
            [drafter.draftset :as ds]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.sparql :as sparql]
            [drafter.user :as user]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :as rdf]
            [grafter-2.rdf4j.repository :as repo :refer [prepare-query]]
            [grafter.url :as url]
            [grafter.vocabularies.rdf :refer :all]
            [drafter.time :as time])
  (:import org.eclipse.rdf4j.model.impl.ContextStatementImpl
           [org.eclipse.rdf4j.query GraphQuery TupleQueryResult TupleQueryResultHandler BindingSet GraphQueryResult]
           org.eclipse.rdf4j.rio.RDFHandler
           java.net.URI))

(defn- create-draftset-statements [user-uri title description draftset-uri created-date]
  (let [ss [draftset-uri
            [rdf:a drafter:DraftSet]
            [drafter:createdAt created-date]
            [drafter:modifiedAt created-date]
            [drafter:version (util/version)]
            [drafter:createdBy user-uri]
            [drafter:hasOwner user-uri]]]
    (cond-> ss
            (some? title) (conj [rdfs:label title])
            (some? description) (conj [rdfs:comment description]))))

(defn create-draftset!
  "Creates a new draftset in the given database and returns its id. If
  no title is provided (i.e. it is nil) a default title will be used
  for the new draftset."
  ([db creator] (create-draftset! db creator nil))
  ([db creator title] (create-draftset! db creator title nil))
  ([db creator title description] (create-draftset! db creator title description util/create-uuid time/system-clock))
  ([db creator title description id-creator clock]
   (with-open [dbcon (repo/->connection db)]
     (let [draftset-id (id-creator)
           created-date (time/now clock)
           user-uri (user/user->uri creator)
           template (create-draftset-statements user-uri title description (url/append-path-segments draftset-uri draftset-id) created-date)
           quads (to-quads template)]
       (rdf/add dbcon quads)
       (ds/->DraftsetId (str draftset-id))))))

(defn with-state-graph
  "Wraps the string in a SPARQL
   GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
     <<sparql-fragment>>
   } clause."

  [& sparql-string]
  (apply str " GRAPH <" mgmt/drafter-state-graph "> { "
         (concat sparql-string
                 " }")))

(defn draftset-exists? [db draftset-ref]
  (let [q (fl/format-query
            {:prefixes mgmt/base-prefixes
             :ask []
             :where [[:graph mgmt/drafter-state-graph
                      [[(-> (ds/->draftset-uri draftset-ref) (url/->java-uri)) :rdf/type :drafter/DraftSet]]]]})]
    (sparql/eager-query db q)))

(defn- delete-draftset-statements-query [draftset-ref]
  (let [ds-uri (url/->java-uri (ds/->draftset-uri draftset-ref))]
    (fl/format-update {:prefixes mgmt/base-prefixes
                       :with mgmt/drafter-state-graph
                       :delete [[ds-uri '?dp '?do]
                                '[?submission ?sp ?so]]
                       :where [[ds-uri :rdf/type :drafter/DraftSet]
                               [ds-uri '?dp '?do]
                               [:optional
                                [[ds-uri :drafter/hasSubmission '?submission]
                                 '[?submission ?sp ?so]]]]}
                      :pretty? true)))

(defn delete-draftset-statements! [db draftset-ref]
  (sparql/update! db (delete-draftset-statements-query draftset-ref)))

(defn- get-draftset-owner-query [draftset-ref]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-ref))]
    (fl/format-query
      {:prefixes mgmt/base-prefixes
       :select ['?owner]
       :where [[:graph mgmt/drafter-state-graph
                [{draftset-uri {:rdf/type #{:drafter/DraftSet}
                                :drafter/hasOwner #{'?owner}}}]]]
       :limit 1}
      :pretty? true)))

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
    :draft-graph-exists (sparql/eager-query repo
                                            (fl/format-query
                                              {:ask []
                                               :where [[:graph dg
                                                        '[[?s ?p ?o]]]]}))))

(defn- get-draftsets-matching-graph-mappings-query [match-clauses]
  (fl/format-query
    {:prefixes mgmt/base-prefixes
     :select :*
     :where [[:graph mgmt/drafter-state-graph
              ['[?ds :rdf/type :drafter/DraftSet]
               '[?dg :drafter/inDraftSet ?ds]
               '[?lg :rdf/type :drafter/ManagedGraph]
               '[?lg :drafter/hasDraft ?dg]
               '[?lg :drafter/isPublic ?public]

               [:where {:select-distinct '[?ds]
                        :where ['[?ds :rdf/type :drafter/DraftSet]
                                (if match-clauses
                                  (vec (cons :union match-clauses))
                                  [:union []])]}]]]]}
    :pretty? true))

(defn- get-draftsets-matching-properties-query [match-clauses]
  (fl/format-query {:prefixes mgmt/base-prefixes
                    :select :*
                    :where [[:graph mgmt/drafter-state-graph
                             ['[?ds :dcterms/created ?created]
                              '[?ds :dcterms/modified ?modified]
                              '[?ds :drafter/version ?version]
                              '[?ds :dcterms/creator ?creator]
                              '[:optional
                                [[?ds :rdfs/comment ?description]]]
                              '[:optional
                                [[?ds :drafter/hasOwner ?owner]]]
                              '[:optional
                                [[?ds :rdfs/label ?title]]]
                              '[:optional
                                [[?ds :drafter/submittedBy ?submitter]]]
                              '[:optional
                                [[?ds :drafter/hasSubmission ?submission]
                                 [?submission :drafter/claimUser ?claimuser]]]
                              '[:optional
                                [[?ds :drafter/hasSubmission ?submission]
                                 [?submission :drafter/claimPermission ?permission]]]
                              ;; Makes the assumption that usernames and permissions don't contain
                              ;; spaces, so we can use space as a separator. Usernames are URIs, which
                              ;; cannot contain spaces. Permissions are OAuth 2.0 scopes, which also
                              ;; cannot contain spaces.
                              '[:optional
                                [[:where {:select [?ds [(group-concat ?p :separator " ") ?viewpermissions]]
                                          :where [[?ds :drafter/viewPermission ?p]]
                                          :group-by [?ds]}]]]
                              '[:optional
                                [[:where {:select [?ds [(group-concat ?u :separator " ") ?viewusers]]
                                          :where [[?ds :drafter/viewUser ?u]]
                                          :group-by [?ds]}]]]


                              [:where {:select-distinct '[?ds]
                                       :where ['[?ds :rdf/type :drafter/DraftSet]
                                               (if match-clauses
                                                 (vec (cons :union match-clauses))
                                                 [:union []])]}]]]]}
                   :pretty? true))

(defn- draftset-uri-clause [draftset-ref]
  [[:values {'?ds [(url/->java-uri (ds/->draftset-uri draftset-ref))]}]])

(defn get-draftset-graph-states [repo draftset-ref]
  (let [q (get-draftsets-matching-graph-mappings-query [(draftset-uri-clause draftset-ref)])]
    (->> q
         (sparql/eager-query repo)
         (map graph-mapping-result->graph-mapping))))

(defn get-draftset-graph-mapping [repo draftset-ref]
  (let [graph-states (get-draftset-graph-states repo draftset-ref)
        mapping-pairs (map (juxt :live-graph-uri :draft-graph-uri) graph-states)]
    (into {} mapping-pairs)))

(defn- draftset-properties-result->properties
  [draftset-ref
   {:keys [created
           title
           description
           creator
           owner
           permission
           claimuser
           submitter
           modified
           version
           viewpermissions
           viewusers] :as _ds}]
  (let [required-fields {:id (str (ds/->draftset-id draftset-ref))
                         :type "Draftset"
                         :created-at created
                         :created-by (user/uri->username creator)
                         :updated-at modified
                         :version version}
        optional-fields {:display-name title
                         :description description
                         :current-owner (some-> owner (user/uri->username))
                         :claim-permission (keyword permission)
                         :claim-role (keyword (user/canonical-permission->role
                                               permission))
                         :claim-user (some-> claimuser (user/uri->username))
                         :submitted-by (some-> submitter (user/uri->username))
                         :view-permissions
                         (when viewpermissions
                           (set (map keyword
                                     (string/split viewpermissions #" "))))
                         :view-users
                         (when viewusers
                           (set (map #(user/uri->username (URI. %))
                                     (string/split viewusers #" "))))}]
    (merge required-fields (remove (comp nil? second) optional-fields))))

(defn- combine-draftset-properties-and-graph-states [ds-properties graph-states]
  (assoc ds-properties :changes (graph-states->changes-map graph-states)))


(defn- combine-all-properties-and-graph-states [draftset-properties graph-states]
  (let [ds-uri->graph-states (group-by :draftset-uri graph-states)]
    (map (fn [{ds-uri :ds :as result}]
           (let [properties (draftset-properties-result->properties (url/->java-uri (ds/->DraftsetURI ds-uri)) result)
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
  (->> (get-all-draftsets-by repo [(draftset-uri-clause draftset-ref)])
       (first)))

(defn is-draftset-viewer? [backend draftset-ref user]
  (user/can-view? user (get-draftset-info backend draftset-ref)))

(defn- delete-draftset-query [draftset-ref draft-graph-uris]
  (let [delete-drafts-query (map mgmt/delete-draft-graph-and-remove-from-state-query draft-graph-uris)
        delete-draftset-query (delete-draftset-statements-query draftset-ref)]
    (util/make-compound-sparql-query (conj delete-drafts-query delete-draftset-query))))

(defn delete-draftset!
  "Deletes a draftset and all of its constituent graphs"
  [db draftset-ref]
  (let [graph-mapping (get-draftset-graph-mapping db draftset-ref)
        draft-graphs (vals graph-mapping)
        delete-query (delete-draftset-query draftset-ref draft-graphs)]
    (sparql/update! db delete-query)))

(def ^:private draftset-param->predicate
  {:display-name rdfs:label
   :description rdfs:comment})

(defn set-draftset-metadata!
  "Takes a map containing new values for various metadata keys and
  updates them on the given draftset."
  [backend draftset-ref meta-map]
  (when-let [po-pairs (vals (util/intersection-with draftset-param->predicate meta-map vector))]
    (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-ref))
          q (fl/format-update
              {:prefixes mgmt/base-prefixes
               :with mgmt/drafter-state-graph
               :delete [[draftset-uri '?p '?o]]
               :insert (into []
                             (map (fn [[p o]]
                                    [draftset-uri p o])
                                  po-pairs))
               :where [[:values {'?p (into [] (map first po-pairs))}]
                       [:optional
                        [[draftset-uri '?p '?o]]]]}
              :pretty? true)]
      (sparql/update! backend q))))

(defn- submit-draftset-to-permission-query
  [draftset-ref submission-id owner permission]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-ref))
        submit-uri (url/->java-uri (submission-id->uri submission-id))
        user-uri (user/user->uri owner)]
    (fl/format-update
      {:prefixes mgmt/base-prefixes
       :with mgmt/drafter-state-graph
       :delete [[draftset-uri :drafter/hasOwner user-uri]
                [draftset-uri :drafter/submittedBy '?submitter]]
       :insert [[submit-uri :rdf/type :drafter/Submission]
                [submit-uri :drafter/claimPermission (name permission)]
                [draftset-uri :drafter/hasSubmission submit-uri]
                [draftset-uri :drafter/submittedBy user-uri]]
       :where [[draftset-uri :rdf/type :drafter/DraftSet]
               [draftset-uri :drafter/hasOwner user-uri]
               [:optional
                [[draftset-uri :drafter/submittedBy '?submitter]]]]}
      :pretty? true)))

(defn submit-draftset-to-permission!
  "Submits a draftset to users with the specified permission.

   Removes the current owner of a draftset and makes it available to be claimed
   by another user with a particular permission. If the given user is not the
   current owner of the draftset, no changes are made."
  [backend draftset-ref owner permission]
  (sparql/update! backend
                  (submit-draftset-to-permission-query
                   draftset-ref (util/create-uuid) owner permission)))

(defn share-draftset-with-permission!
  "Shares a draftset with users with the specified permission. If the given
   user is not the current owner of the draftset, no changes are made."
  [backend draftset-ref owner permission]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-ref))]
    (sparql/update! backend
                    (fl/format-update
                      {:prefixes mgmt/base-prefixes
                       :with mgmt/drafter-state-graph
                       :insert [[draftset-uri :drafter/viewPermission (name permission)]]
                       :where [[draftset-uri :rdf/type :drafter/DraftSet]
                               [draftset-uri :drafter/hasOwner (user/user->uri owner)]]}
                      :pretty? true))))

(defn unshare-draftset!
  "Removes all shares from a draftset, so only the owner can view it. If the
   given user is not the current owner of the draftset, no changes are made."
  [backend draftset-ref owner]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-ref))]
    (sparql/update! backend
      (fl/format-update
        {:prefixes mgmt/base-prefixes
         :with mgmt/drafter-state-graph
         :delete [{draftset-uri {:drafter/viewPermission #{'?vp}
                                 :drafter/viewUser #{'?vu}}}]
         :where [{draftset-uri {:drafter/hasOwner #{(user/user->uri owner)}
                                :rdf/type #{:drafter/DraftSet}}}
                 [:optional
                  [[draftset-uri :drafter/viewPermission '?vp]]]
                 [:optional
                  [[draftset-uri :drafter/viewUser '?vu]]]]}
        :pretty? true))))

(defn- submit-to-user-query [draftset-ref submission-id submitter target]
  (let [submitter-uri (user/user->uri submitter)
        target-uri (user/user->uri target)
        submit-uri (url/->java-uri (submission-id->uri submission-id))
        draftset-uri (url/->java-uri (ds/->draftset-uri draftset-ref))]
    (fl/format-update
      {:prefixes mgmt/base-prefixes
       :with mgmt/drafter-state-graph
       :delete [[draftset-uri :drafter/hasOwner submitter-uri]
                [draftset-uri :drafter/submittedBy '?submitter]]
       :insert [[submit-uri :rdf/type :drafter/Submission]
                [submit-uri :drafter/claimUser target-uri]
                [draftset-uri :drafter/hasSubmission submit-uri]
                [draftset-uri :drafter/submittedBy submitter-uri]]
       :where [[draftset-uri :rdf/type :drafter/DraftSet]
               [draftset-uri :drafter/hasOwner submitter-uri]
               [:optional
                [[draftset-uri :drafter/submittedBy '?submitter]]]]}
      :pretty? true)))

(defn submit-draftset-to-user! [backend draftset-ref submitter target]
  (let [q (submit-to-user-query draftset-ref (util/create-uuid) submitter target)]
    (sparql/update! backend q)))

(defn share-draftset-with-user! [backend draftset-ref submitter target]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-ref))]
    (sparql/update! backend
                    (fl/format-update
                      {:prefixes mgmt/base-prefixes
                       :with mgmt/drafter-state-graph
                       :insert [[draftset-uri :drafter/viewUser (user/user->uri target)]]
                       :where [[draftset-uri :rdf/type :drafter/DraftSet]
                               [draftset-uri :drafter/hasOwner (user/user->uri submitter)]]}
                      :pretty? true))))

(defn- try-claim-draftset!
  "Sets the claiming user to the owner of the given draftset if the draftset is
   available (has no current owner). We should have already checked the
   claimant has permission to claim the draftset."
  [backend draftset-ref claimant]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-ref))
        user-uri (user/user->uri claimant)]
    (sparql/update! backend
                    (fl/format-update
                      {:prefixes mgmt/base-prefixes
                       :with mgmt/drafter-state-graph
                       :delete [[draftset-uri :drafter/hasSubmission '?submission]
                                '[?submission ?sp ?so]]
                       :insert [[draftset-uri :drafter/hasOwner user-uri]]
                       :where [[draftset-uri :rdf/type :drafter/DraftSet]
                               [draftset-uri :drafter/hasSubmission '?submission]
                               '[?submission ?sp ?so]]}
                      :pretty? true))))

(defn- infer-claim-outcome
  [{:keys [current-owner] :as _ds-info} claimant]
  (if (= (user/username claimant) current-owner) :ok :unknown))

(defn claim-draftset!
  "Attempts to claim a draftset for a user. If the draftset is
  available for claim by the claiming user they will be updated to be
  the new owner. Returns a pair containing the outcome of the
  operation and the current info for the draftset.
  The possible outcomes are:
    - :ok The draftset was claimed by the user
    - :not-found Claim failed because the draftset does not exist
    - :forbidden Claim failed because the user does not have permission
    - :unknown Claim failed for an unknown reason"
  [backend draftset-ref claimant]
  (if-let [ds-info (get-draftset-info backend draftset-ref)]
    (if (user/can-claim? claimant ds-info)
      (do
        (try-claim-draftset! backend draftset-ref claimant)
        (let [ds-info (get-draftset-info backend draftset-ref)
              outcome (infer-claim-outcome ds-info claimant)]
          [outcome ds-info]))
      [:forbidden nil])
    [:not-found nil]))

(defn find-permitted-draftset-operations [backend draftset-ref user]
  (if-let [ds-info (get-draftset-info backend draftset-ref)]
    (user/permitted-draftset-operations ds-info user)
    #{}))

(defn find-draftset-draft-graph
  "Finds the draft graph for a live graph inside a draftset if one
  exists. Returns nil if the draftset does not exist, or does not
  contain a draft for the graph."
  [backend draftset-ref live-graph]
  (let [draftset-uri (url/->java-uri (ds/->draftset-uri draftset-ref))
        [result] (sparql/eager-query backend
                                     (fl/format-query
                                       {:prefixes mgmt/base-prefixes
                                        :select ['?dg]
                                        :where [[:graph mgmt/drafter-state-graph
                                                 [[draftset-uri :rdf/type :drafter/DraftSet]
                                                  ['?dg :rdf/type :drafter/DraftGraph]
                                                  ['?dg :drafter/inDraftSet draftset-uri]
                                                  {live-graph {:rdf/type #{:drafter/ManagedGraph}
                                                               :drafter/hasDraft #{'?dg}}}]]]
                                        :limit 1}
                                       :pretty? true))]
    (:dg result)))

(defn- spog-bindings->statement [^BindingSet bindings]
  (let [subj (.getValue bindings "s")
        pred (.getValue bindings "p")
        obj (.getValue bindings "o")
        graph (.getValue bindings "g")]
    (ContextStatementImpl. subj pred obj graph)))

(defn- rdf-handler->spog-tuple-handler [conn ^RDFHandler rdf-handler]
  (reify
    TupleQueryResult
    (getBindingNames [this]
      ;; hard coded as part of this test stub
      ["s" "p" "o" "g"])

    TupleQueryResultHandler
    (handleSolution [this bindings]
      (let [stmt (spog-bindings->statement bindings)]
        (.handleStatement rdf-handler stmt)))

    (handleBoolean [this b])
    (handleLinks [this links])
    (startQueryResult [this binding-names]
      (.startRDF rdf-handler))
    (endQueryResult [this]
      (.endRDF rdf-handler)
      (.close conn))))

(defn- spog-tuple-query-result->graph-query-result
  "Returns a GraphQueryResult which maps each ?s ?p ?o ?g binding from an inner
   TupleQueryResult into a statement."
  [^TupleQueryResult tqr]
  (reify GraphQueryResult
    (hasNext [_this] (.hasNext tqr))
    (next [_this] (spog-bindings->statement (.next tqr)))
    (remove [_this] (.remove tqr))
    (close [_this] (.close tqr))))

(defn- spog-tuple-query->graph-query
  "Returns a GraphQuery which wraps a ?s ?p ?o ?g TupleQuery and converts each spog binding
   into a statement."
  [conn tuple-query]
  (reify GraphQuery
    (evaluate [_this]
      (let [inner-result (.evaluate tuple-query)]
        (spog-tuple-query-result->graph-query-result inner-result)))
    (evaluate [this rdf-handler]
      (.evaluate tuple-query (rdf-handler->spog-tuple-handler conn rdf-handler)))
    (setBinding [_this name value]
      (.setBinding tuple-query name value))
    (removeBinding [_this name]
      (.removeBinding tuple-query name))
    (clearBindings [_this]
      (.clearBindings tuple-query))
    (getBindings [_this]
      (.getBindings tuple-query))
    (setDataset [_this dataset]
      (.setDataset tuple-query dataset))
    (getDataset [_this]
      (.getDataset tuple-query))
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
        tuple-query (repo/prepare-query conn
                                        (fl/format-query {:select :*
                                                          :where '[[:graph ?g [[?s ?p ?o]]]]}))]
    (spog-tuple-query->graph-query conn tuple-query)))

(defn all-graph-triples-query [backend graph]
  (let [g (URI. (str graph))
        conn (repo/->connection backend)]
    (prepare-query conn
                   (fl/format-query {:construct '[[?s ?p ?o]]
                                     :where [[:graph g '[[?s ?p ?o]]]]}))))
