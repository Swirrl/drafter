(ns drafter.backend.draftset.draft-management
  (:require
    [clojure.set :as set]
    [clojure.tools.logging :as log]
    [com.yetanalytics.flint :as fl]
    [grafter.vocabularies.rdf :refer [rdf]]
    [drafter.rdf.drafter-ontology :refer [drafter drafter:isPublic]]
    [drafter.rdf.sparql :as sparql :refer [update!]]
    [drafter.time :as time]
    [drafter.util :as util]
    [grafter-2.rdf4j.io :as rio]
    [grafter-2.rdf4j.repository :as repo]
    [grafter-2.rdf4j.templater :refer [graph]]
    [grafter.url :as url]
    [grafter.vocabularies.rdf :refer :all])
  (:import java.net.URI
           [java.util UUID]))

(def base-prefixes {:rdf (rdf "")
                    :rdfs (rdfs "")
                    :dcterms (URI. "http://purl.org/dc/terms/")
                    :drafter drafter})

(def drafter-state-graph (URI. "http://publishmydata.com/graphs/drafter/drafts"))

(def staging-base (URI. "http://publishmydata.com/graphs/drafter/draft"))

(def to-quads (partial graph drafter-state-graph))

(defn make-draft-graph-uri []
  (url/->java-uri (url/append-path-segments staging-base (str (UUID/randomUUID)))))

(defn is-graph-managed? [db graph-uri]
  (sparql/eager-query db
                      (fl/format-query {:prefixes base-prefixes
                                        :ask []
                                        :where [[:graph drafter-state-graph
                                                 [[graph-uri :rdf/type :drafter/ManagedGraph]]]]})))

(defn is-graph-live? [db graph-uri]
  (sparql/eager-query db
                      (fl/format-query {:prefixes base-prefixes
                                        :ask []
                                        :where [[:graph drafter-state-graph
                                                 [[graph-uri :rdf/type :drafter/ManagedGraph]
                                                  [graph-uri :drafter/isPublic true]]]]})))

(defn- has-more-than-one-draft?
  "Given a live graph uri, check to see if it is referenced by more
  than one draft in the state graph."
  [db live-graph-uri]
  (sparql/eager-query db
                      (fl/format-query {:prefixes base-prefixes
                                        :ask []
                                        :where {:select '[[(count ?draft) ?numberOfRefs]]
                                                :where [{live-graph-uri {:rdf/type #{:drafter/ManagedGraph}
                                                                         :drafter/hasDraft #{'?draft}}}]
                                                :having '[(> ?numberOfRefs 1)]}}
                                       :pretty? true)))

(defn xsd-datetime
  "Coerce a date into the xsd-datetime string"
  [offset-datetime]
  (str (rio/->backend-type offset-datetime)))

(defn set-timestamp-sparql
  "Returns an update string to update the given subject/resource with
  the supplied a timestamp."
  [subject time-predicate datetime]
  (let [inst (.toInstant datetime)]
    (fl/format-update {:prefixes {:xsd "<http://www.w3.org/2001/XMLSchema#>"}
                       :with drafter-state-graph
                       :delete [[subject time-predicate '?lastvalue]]
                       :insert [[subject time-predicate inst]]
                       :where [[:optional
                                [[subject time-predicate '?lastvalue]]]]}
                      :force-iris? true
                      :pretty? true)))

(defn set-version-sparql
  "Returns an update string to update the given subject/resource with
   the supplied version."
  [subject version]
  (fl/format-update {:prefixes base-prefixes
                     :with drafter-state-graph
                     :delete [[subject :drafter/version '?lastvalue]]
                     :insert [[subject :drafter/version version]]
                     :where [[:optional
                              [[subject :drafter/version '?lastvalue]]]]}
                    :pretty? true))

(defn- upsert-single-object-sparql [subject predicate object]
  (fl/format-update {:with drafter-state-graph
                     :delete [[subject predicate '?o]]
                     :insert [[subject predicate object]]
                     :where [[:optional
                              [[subject predicate '?o]]]]}
                    :pretty? true))

(defn upsert-single-object!
  "Inserts or updates the single object for a given predicate and subject in the state graph"
  [db subject predicate object]
  (update! db (upsert-single-object-sparql subject predicate object)))

(defn set-isPublic-query [live-graph-uri is-public]
  (upsert-single-object-sparql live-graph-uri drafter:isPublic is-public))

(defn set-isPublic! [db live-graph-uri boolean-value]
  (upsert-single-object! db live-graph-uri drafter:isPublic boolean-value))

(defn delete-graph-contents-query [graph-uri]
  (fl/format-update {:drop-silent [:graph graph-uri]}))

(defn delete-graph-contents!
  "Transactionally delete the contents of the supplied graph and set
  its modified time to the supplied instant.

  Note modified-at is an instant not a 0-arg clock-fn."
  [db graph-uri]
  (update! db (delete-graph-contents-query graph-uri))
  (log/info (str "Deleted graph " graph-uri)))

(defn delete-draft-state-query-sparql [draft-graph-uri]
  ;; if the graph-uri is a draft graph uri,
  ;; remove the mention of this draft uri, but leave the live graph as a managed graph.
  (fl/format-update {:prefixes base-prefixes
                     :with drafter-state-graph
                     :delete [['?live :drafter/hasDraft draft-graph-uri]
                              [draft-graph-uri '?p '?o]]
                     :where [{'?live {:rdf/type #{:drafter/ManagedGraph}
                                      :drafter/hasDraft #{draft-graph-uri}}}
                             [draft-graph-uri '?p '?o]]}
                    :pretty? true))

(defn delete-draft-graph-and-remove-from-state-query [draft-graph-uri]
  (util/make-compound-sparql-query [(delete-graph-contents-query draft-graph-uri)
                                    (delete-draft-state-query-sparql draft-graph-uri)]))

(defn- delete-dependent-private-managed-graph-query [draft-graph-uri]
  (fl/format-update {:prefixes base-prefixes
                     :with drafter-state-graph
                     :delete '[[?lg ?lp ?lo]]
                     :where [{'?lg {:rdf/type #{:drafter/ManagedGraph}
                                    :drafter/isPublic #{false}}}
                             ['?lg '?lp '?lo]

                             [:minus
                              [['?lg :drafter/hasDraft '?odg]
                               [:bind [draft-graph-uri '?draftGraph]]
                               [:filter '(not= ?draftGraph ?odg)]]]]}
                    :pretty? true))

(defn- delete-draft-graph-query [draft-graph-uri]
  (let [q (util/make-compound-sparql-query
           [(delete-draft-graph-and-remove-from-state-query draft-graph-uri)
            (delete-dependent-private-managed-graph-query draft-graph-uri)])]
    q))

(defn delete-draft-graph!
  "Deletes a draft graph's contents and all references to it in the
  state graph. If its associated managed graph is private and has only
  the given draft graph then it will also be removed."
  [db draft-graph-uri]
  (update! db (delete-draft-graph-query draft-graph-uri)))

(defn lookup-live-graph [db draft-graph-uri]
  "Given a draft graph URI, lookup and return its live graph. Returns nil if not
  found."
  (let [q (fl/format-query
            {:prefixes base-prefixes
             :select ['?live]
             :where [[:graph drafter-state-graph
                      [{'?live {:rdf/type #{:drafter/ManagedGraph}
                                :drafter/hasDraft #{draft-graph-uri}}}]]]
             :limit 1}
            :pretty? true)]
    (-> (sparql/eager-query db q)
        first
        (:live))))

(defn- delete-live-graph-from-state-query [live-graph-uri]
  (fl/format-update
    {:prefixes base-prefixes
     :delete-where [[:graph drafter-state-graph
                     [[live-graph-uri :rdf/type :drafter/ManagedGraph]
                      [live-graph-uri '?p '?o]]]]}
    :pretty? true))

(defn copy-graph
  "Copies source graph to destination graph.  Accepts an optional map
  of options.

  Currently, the only supported option is the boolean value :silent
  which will ensure the copy always succeeds, whether or not their is
  a source graph etc."
  ([repo from to] (copy-graph repo from to {}))
  ([repo from to {:keys [silent] :as _opts :or {silent false}}]
   (update! repo (fl/format-update
                   {(if silent :copy-silent :copy) from
                    :to to}))))

(defn live-graphs
  "Get all live graph names.  Takes an optional boolean keyword
  argument of :online to allow querying for all online/offline live
  graphs."
  [db & {:keys [online] :or {online true}}]
  (->> (fl/format-query
         {:prefixes base-prefixes
          :select ['?live]
          :where [[:graph drafter-state-graph
                   [{'?live {:rdf/type #{:drafter/ManagedGraph}
                             :drafter/isPublic #{online}}}]]]}
         :pretty? true)
       (sparql/eager-query db)
       (map :live)
       (into #{})))

(defn graph-non-empty?
  "Returns true if the graph contains any statements."
  [repo graph-uri]
  (sparql/eager-query repo
                      (fl/format-query
                        {:ask []
                         :where {:select :*
                                 :where [[:graph graph-uri
                                          '[[?s ?p ?o]]]]
                                 :limit 1}})))

(defn graph-empty?
  "Returns true if there are no statements in the associated graph."
  [repo graph-uri]
  (not (graph-non-empty? repo graph-uri)))

(defn should-delete-live-graph-from-state-after-draft-migrate?
  "When migrating a draft graph to live, the associated 'is managed
  graph' statement should be removed from the state if graph if:
  1. The migrate operation is a delete (i.e. the draft graph is empty)
  2. The migrated graph is the only draft associated with the live
  graph."
  [repo draft-graph-uri live-graph-uri]
  (and
   (graph-empty? repo draft-graph-uri)
   (not (has-more-than-one-draft? repo live-graph-uri))))

(defn- update-live-graph-timestamps-query
  "set a dcterms:issued timestamp if it doesn't have one already"
  [draft-graph-uri now-ts]
  (fl/format-update
    {:prefixes base-prefixes
     :with drafter-state-graph
     :insert [['?live :dcterms/issued now-ts]]

     :where [[:values {'?draft [draft-graph-uri]}]
             {'?live {:rdf/type #{:drafter/ManagedGraph}
                      :drafter/hasDraft #{'?draft}}}
             [:filter '(not-exists [[?live :dcterms/issued ?existing]])]]}
    :pretty? true))

(defn- move-graph
  "Move's how TBL intended.  Issues a SPARQL MOVE query.
  Note this is super slow on stardog 3.1."
  [source destination]
  ;; Move's how TBL intended...
  (str "MOVE SILENT <" source "> TO <" destination ">"))

;;Repository -> String -> { queries: [String], live-graph-uri: String }
(defn- migrate-live-queries [db draft-graph-uri transaction-at]
  (if-let [live-graph-uri (lookup-live-graph db draft-graph-uri)]
    (let [move-query (move-graph draft-graph-uri live-graph-uri)
          update-timestamps-query (update-live-graph-timestamps-query draft-graph-uri transaction-at)
          delete-state-query (delete-draft-state-query-sparql draft-graph-uri)
          live-public-query (set-isPublic-query live-graph-uri true)
          queries [update-timestamps-query move-query delete-state-query live-public-query]
          queries (if (should-delete-live-graph-from-state-after-draft-migrate? db draft-graph-uri live-graph-uri)
                    (conj queries (delete-live-graph-from-state-query live-graph-uri))
                    queries)]
      {:queries queries
       :live-graph-uri live-graph-uri})))

(defn- get-flint-rewrite-state-graph-pattern
  "Returns a group graph pattern within the drafter state graph which finds the draftset graphs
   to rewrite within, along with the corresponding live and draft graphs to rewrite. The graph pattern
   binds the following query variables:

     ?ds - Draftset URI
     ?g - (Draft) graph to search within the draftset identified by ?ds
     ?dg - A draft graph in the draftset identified by ?ds
     ?lg - A live graph with draft ?dg in the draftset identified by ?ds."
  [{:keys [deleted live-graph-uris draft-graph-uris draftset-uri]}]
  (let [ds-values (when draftset-uri {'?ds [draftset-uri]})
        live-values (when (seq live-graph-uris) {'?lg live-graph-uris})
        draft-values (when (seq draft-graph-uris) {'?dg draft-graph-uris})
        filter-exp (case deleted
                     :ignore '(exists [[:graph ?dg
                                        [[?s_ ?p_ ?o_]]]])
                     :rewrite '(not-exists [[:graph ?dg
                                             [[?s_ ?p_ ?o_]]]])
                     nil)]
    (cond-> [[:graph drafter-state-graph
              '[[?ds :rdf/type :drafter/DraftSet]
                [?g :drafter/inDraftSet ?ds]
                [?dg :drafter/inDraftSet ?ds]
                [?lg :drafter/hasDraft ?dg]]]]

            filter-exp (conj [:filter filter-exp])
            ds-values (conj [:values ds-values])
            live-values (conj [:values live-values])
            draft-values (conj [:values draft-values]))))

(defn- rewrite-subjects-q
  "Rewrites all values in subject position to/from their value in draft/live"
  [{:keys [?from ?to] :as opts}]
  (let [state-filter-pattern (get-flint-rewrite-state-graph-pattern opts)]
    (fl/format-update
      {:prefixes base-prefixes
       :delete [[:graph '?g
                 [[?from '?p '?o]]]]
       :insert [[:graph '?g
                 [[?to '?p '?o]]]]
       :where (->> [[:graph '?g
                     [[?from '?p '?o]]]]
                   (concat state-filter-pattern)
                   (into []))}
      :pretty? true)))

(defn- rewrite-predicates-q
  "Rewrites all values in predicate position to/from their value in draft/live"
  [{:keys [?from ?to] :as opts}]
  (let [state-filter-pattern (get-flint-rewrite-state-graph-pattern opts)]
    (fl/format-update
      {:prefixes base-prefixes
       :delete [[:graph '?g
                 [['?s ?from '?o]]]]
       :insert [[:graph '?g
                 [['?s ?to '?o]]]]
       :where (->> [[:graph '?g
                     [['?s ?from '?o]]]]
                   (concat state-filter-pattern)
                   (into []))}
      :pretty? true)))

(defn- rewrite-objects-q
  "Rewrites all values in object position to/from their value in draft/live"
  [{:keys [?from ?to] :as opts}]
  (let [state-filter-pattern (get-flint-rewrite-state-graph-pattern opts)]
    (fl/format-update
      {:prefixes base-prefixes
       :delete [[:graph '?g
                 [['?s '?p ?from]]]]
       :insert [[:graph '?g
                 [['?s '?p ?to]]]]
       :where (->> [[:graph '?g
                     [['?s '?p ?from]]]]
                   (concat state-filter-pattern)
                   (into []))}
      :pretty? true)))

(defn rewrite-q
  "Generates a query to rewrite graph values within a draftset from draft to live or vice versa.
   The query generates bindings for ?lg and ?dg representing the live and draft graphs within a
   draftset. The :?from and :?to parameters can be specified to control the direction of rewriting:
   {:?from ?lg :?to ?dg} will convert live graph URIs to their corresponding draft graphs, and
   {:?from ?dg :to ?lg} will convert draft graphs to their live counterparts.

   In addition the following other options are supported:
     :deleted - Graphs deleted within a draftset retain an entry in the state graph but their draft data graphs
                are empty. Setting this parameter to :ignore will filter all empty draft graphs and any references
                to them will not be rewritten. If set to :rewrite then ONLY references to empty (i.e. deleted)
                draft graphs will be rewritten.

     :draftset-uri - Set this to restrict the draftset to be rewritten.

     :live-graph-uris - A collection of live graph URIs to rewrite references to.

     :draft-graph-uris - A collection of draft graph URIs to rewrite references to."
  [opts]
  (let [qs [(rewrite-subjects-q opts)
            (rewrite-predicates-q opts)
            (rewrite-objects-q opts)]]
    (util/make-compound-sparql-query qs)))

(defn rewrite-draftset-q [opts]
  (rewrite-q (assoc opts :?from '?lg :?to '?dg :deleted :ignore)))

(defn- unrewrite-draftset-q [opts]
  (rewrite-q (assoc opts :?from '?dg :?to '?lg)))

(defn rewrite-draftset! [conn opts]
  (->> (rewrite-draftset-q opts)
       (sparql/update! conn)))

(defn unrewrite-draftset! [conn opts]
  (let [q (unrewrite-draftset-q opts)]
    (sparql/update! conn q)))

(defn migrate-graphs-to-live! [repo graphs clock]
  "Migrates a collection of draft graphs to live through a single
  compound SPARQL update statement. Explicit UPDATE statements do not
  take part in transactions on the remote sesame SPARQL client."
  (log/info "Starting make-live for graphs " graphs)
  (when (seq graphs)
    (let [transaction-started-at (time/now clock)
          graph-migrate-queries (mapcat #(:queries (migrate-live-queries repo % transaction-started-at))
                                        graphs)
          fixup-q (unrewrite-draftset-q {:draft-graph-uris graphs})
          update-str (util/make-compound-sparql-query (cons fixup-q graph-migrate-queries))]
      (update! repo update-str)))
  (log/info "Make-live for graph(s) " graphs " done"))

(defn calculate-draft-raw-graphs
  "Returns the set of draft data graphs given the set of all visible live graphs,
  the set of graphs which have a draft graph in the draftset and the corresponding
  set of draft graphs."
  [public-live-graphs live-graph-drafts draft-graphs]
  (set/union
    (set/difference (set public-live-graphs) (set live-graph-drafts))
    (set draft-graphs)))

(defn draft-raw-graphs
  "Returns a set of all the raw graphs within a draftset i.e. all the draft graphs and all the
   visible live graphs. If union-with-live? is false there are no visible live graphs, otherwise
   all the live graphs without a corresponding draft graph in the draftset are included."
  [db graph-mapping union-with-live?]
  (let [live-graphs (if union-with-live? (live-graphs db) #{})]
    (calculate-draft-raw-graphs live-graphs (keys graph-mapping) (vals graph-mapping))))

(defn append-data-batch!
  "Appends a sequence of triples to the given draft graph."
  [repo graph-uri triple-batch]
  ;;NOTE: The remote sesame client throws an exception if an empty transaction is committed
  ;;so only create one if there is data in the batch
  (if-not (empty? triple-batch)
    ;;WARNING: This assumes the backend is a sesame backend which is
    ;;true for all current backends.
    (with-open [conn (repo/->connection repo)]
      (repo/with-transaction conn
        (sparql/add conn graph-uri triple-batch)))))
