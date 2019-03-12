(ns drafter.backend.common
  (:require [drafter.backend.draftset.arq :as arq]
            [grafter.rdf.protocols :as proto]
            [grafter.rdf4j.repository :as repo]
            [clojure.set :as set])
  (:import java.net.URI
           org.eclipse.rdf4j.query.Dataset
           org.apache.jena.query.Query
           org.apache.jena.sparql.core.DatasetDescription))

(defprotocol SparqlExecutor
  (prepare-query [this sparql-string]))

(defprotocol ToRepository
  (->sesame-repo [this]
    "Gets the sesame repository for this backend"))

(defn stop-backend [backend]
  (repo/shutdown (->sesame-repo backend)))

(defn ->repo-connection
  "Opens a connection to the underlying Sesame repository for the
  given backend."
  [backend]
  (repo/->connection (->sesame-repo backend)))


(def itriple-readable-delegate
  {:to-statements (fn [this options]
                    (proto/to-statements (->sesame-repo this) options))})

(def isparqlable-delegate
  {:query-dataset (fn [this sparql-string model]
                    (proto/query-dataset (->sesame-repo this) sparql-string model))})

(def isparql-updateable-delegate
  {:update! (fn [this sparql-string]
              (proto/update! (->sesame-repo this) sparql-string))})

(def to-connection-delegate
  {:->connection (fn [this]
                   (repo/->connection (->sesame-repo this)))})

(defn- add-delegate
  ([this triples] (proto/add (->sesame-repo this) triples))
  ([this graph triples] (proto/add (->sesame-repo this) graph triples))
  ([this graph format triple-stream] (proto/add (->sesame-repo this) graph format triple-stream))
  ([this graph base-uri format triple-stream] (proto/add (->sesame-repo this) graph base-uri format triple-stream)))

(defn- add-statement-delegate
  ([this statement] (proto/add-statement (->sesame-repo this) statement))
  ([this graph statement] (proto/add-statement (->sesame-repo this) graph statement)))

(def itriple-writeable-delegate
  {:add add-delegate
   :add-statement add-statement-delegate})

(defn- get-restrictions [graph-restrictions]
  (cond
   (coll? graph-restrictions) graph-restrictions
   (fn? graph-restrictions) (graph-restrictions)
   :else nil))

(defn restricted-dataset
  "Returns a restricted dataset or nil when given either a 0-arg
  function or a collection of graph uris."
  [graph-restrictions]
  {:pre [(or (nil? graph-restrictions)
             (coll? graph-restrictions)
             (fn? graph-restrictions))]
   :post [(or (instance? Dataset %)
              (nil? %))]}
  (when-let [graph-restrictions (get-restrictions graph-restrictions)]
    (let [stringified-restriction (map str graph-restrictions)]
      (repo/make-restricted-dataset :default-graph stringified-restriction
                                    :named-graphs stringified-restriction))))

(defn apply-restriction [pquery restriction]
  (let [dataset (restricted-dataset restriction)]
    (.setDataset pquery dataset)
    pquery))


(defn validate-query
  "Validates a query by parsing it using ARQ. If the query is invalid
  a QueryParseException is thrown."
  [query-str]
  (arq/sparql-string->arq-query query-str)
  query-str)


(defn prep-and-validate-query [conn sparql-string]
  (let [;;repo (->sesame-repo backend)
        ;; Technically calls to live endpoint don't need to be
        ;; validated with JENA/ARQ but as draftsets do their rewriting
        ;; through ARQ this helps ensure consistency between
        ;; implementations.
        validated-query-string (validate-query sparql-string)]
    (repo/prepare-query conn validated-query-string)))

(defmulti default-graphs class)
(defmethod default-graphs Dataset [ds] (.getDefaultGraphs ds))
(defmethod default-graphs DatasetDescription [ds] (.getDefaultGraphURIs ds))

(defmulti named-graphs class)
(defmethod named-graphs Dataset [ds] (.getNamedGraphs ds))
(defmethod named-graphs DatasetDescription [ds] (.getNamedGraphURIs ds))

(defn dataset->restriction "
  ;: Dataset -> {:default-graph (Set String) :named-graphs (Set String)}"
  [ds]
  (when ds
    (let [default-graphs (default-graphs ds)
          named-graphs (named-graphs ds)]
      (cond-> {}
        default-graphs (assoc :default-graph default-graphs)
        named-graphs (assoc :named-graphs named-graphs)))))

(defn query-dataset-restriction "
  ;: Query -> {:default-graph (Set String) :named-graphs (Set String)}"
  [query]
  (dataset->restriction (.getDatasetDescription query)))

(defn normalize-restriction [graph-restrictions]
  {:pre [(or (nil? graph-restrictions)
             (coll? graph-restrictions)
             (fn? graph-restrictions))]}
  (when-let [graph-restrictions (get-restrictions graph-restrictions)]
    (let [stringified-restriction (set (map str graph-restrictions))]
      {:default-graph stringified-restriction
       :named-graphs stringified-restriction})))

(defn stringify-restriction [restriction]
  (cond-> restriction
    (:default-graph restriction)
    (update :default-graph (comp set (partial map str)))
    (:named-graphs restriction)
    (update :named-graphs (comp set (partial map str)))))

(defn some-restriction [restriction]
  (when (or (seq (:default-graph restriction))
            (seq (:named-graphs restriction)))
    restriction))

(defn restriction-intersection
  [{default-a :default-graph named-a :named-graphs}
   {default-b :default-graph named-b :named-graphs}]
  (let [restricted-graphs (set/intersection (set/union default-a named-a)
                                            (set/union default-b named-b))]
    {:default-graph (set/intersection restricted-graphs default-a default-b)
     :named-graphs (set/intersection restricted-graphs named-a named-b)}))

(defn restrict-query
  [pquery user-restriction query-dataset-restriction graph-restriction]
  (letfn [(do-restrict [query restriction]
            (doto pquery
              (.setDataset (repo/make-restricted-dataset
                            :default-graph (:default-graph restriction)
                            :named-graphs (:named-graphs restriction)))))]
    (let [graph-restriction (normalize-restriction graph-restriction)]
      (if-let [restriction (some->> (or (some-restriction user-restriction)
                                        (some-restriction query-dataset-restriction))
                             (stringify-restriction)
                             (restriction-intersection graph-restriction))]
        (do-restrict pquery restriction)
        (do-restrict pquery graph-restriction)))))

(defn user-dataset [{:keys [default-graph-uri named-graph-uri] :as sparql}]
  (when (or (seq default-graph-uri) (seq named-graph-uri))
    (repo/make-restricted-dataset :default-graph default-graph-uri
                                  :named-graphs named-graph-uri)))


(comment
  ;; experiments




  (let [at (atom [])
        listener (reify org.eclipse.rdf4j.repository.event.RepositoryConnectionListener
                   (add [this conn sub pred obj graphs]
                     (swap! at conj [:add sub pred obj graphs]))
                   (begin [this conn]
                     (swap! at conj [:begin]))
                   (close [this conn]
                     (swap! at conj [:close]))
                   (commit [this conn]
                     (swap! at conj [:commit])))

        repo (doto (grafter.rdf.repository/notifying-repo (grafter.rdf.repository/sparql-repo "http://localhost:5820/drafter-test-db/query" "http://localhost:5820/drafter-test-db/update"))
               (.addRepositoryConnectionListener listener))]

    (with-open [conn (grafter.rdf.repository/->connection repo)]
      (grafter.rdf/add conn [(grafter.rdf.protocols/->Quad (URI. "http://foo") (URI. "http://foo") (URI. "http://foo") (URI. "http://foo"))]))

    @at)

  ;; => [[:begin] [:add #object[org.eclipse.rdf4j.model.impl.URIImpl 0x4e213334 "http://foo"] #object[org.eclipse.rdf4j.model.impl.URIImpl 0x211a94a8 "http://foo"] #object[org.eclipse.rdf4j.model.impl.URIImpl 0x667e19c8 "http://foo"] #object["[Lorg.eclipse.rdf4j.model.Resource;" 0x2bfcfcbe "[Lorg.eclipse.rdf4j.model.Resource;@2bfcfcbe"]] [:commit] [:close]]


  )
