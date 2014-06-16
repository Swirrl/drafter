(ns drafter.rdf.jena
  (:import [com.hp.hpl.jena.tdb TDBFactory TDB]
           [com.hp.hpl.jena.query
            Dataset QueryFactory QueryExecutionFactory
            QueryExecution Syntax Query ReadWrite
            ResultSetFormatter ]
           [com.hp.hpl.jena.update GraphStore GraphStoreFactory UpdateExecutionFactory
            UpdateFactory UpdateProcessor UpdateRequest]
           [com.hp.hpl.jena.rdf.model ModelFactory]
           [org.apache.jena.riot Lang RDFDataMgr])
  (:require [pandect.core :as digest]))

(defprotocol ToJenaTripleStore
  (->jena-triple-store [this]))

(extend-protocol ToJenaTripleStore
  String
  (->jena-triple-store [this]
    (TDBFactory/createDataset this))

  Dataset
  (->jena-triple-store [this]
    this))

(defprotocol ToSparqlQuery
  (->query [this]))

(extend-protocol ToSparqlQuery
  String
  (->query [this]
    (QueryFactory/create this))

  QueryExecution
  (->query [this]
    (.getQuery this))

  Query
  (->query [this]
    this))

(defn build-query-execution [dataset query-str & {:keys [union-default-graph] :or {union-default-graph true}}]
  (let [query (QueryFactory/create query-str)
        query-execution (doto (QueryExecutionFactory/create query dataset))
        query-context (.getContext query-execution)]
    (doto query-context
      (.set TDB/symUnionDefaultGraph true))
    query-execution))

(defn run-query-execution [query-execution]
  (let [query (->query query-execution)
        results (cond
                 (.isAskType query) (-> query-execution .execAsk)
                 (.isConstructType query) (-> query-execution .execConstructTriples iterator-seq)
                 (.isDescribeType query) (-> query-execution .execDescribe)
                 (.isSelectType query)  (-> query-execution .execSelect iterator-seq))]
    results))

(defmacro run-query [dataset [binding query-form] & body]
  "Wraps a read query in a transaction and cleans up after itself.
  Always returns nill as it assumes the results are side-effecting.

e.g. (doquery db1 [results \"SELECT * WHERE {?s ?p ?o} LIMIT 10\"] (doseq [result results] (println result)) )
"
  `(with-transaction :read ~dataset
     (let [queryexecution# (build-query-execution ~dataset ~query-form)
           ~binding (run-query-execution queryexecution#)
           results# ~@body]
       (.close queryexecution#)
       results#)))

(defmacro query-seq [dataset [binding query-form] & body]
  "Like run-query but wraps results in a doall to force results to be
  loaded and returned after the connection & queries are closed."
  `(run-query ~dataset [~binding ~query-form]
              (doall
               ~@body)))


(defn- read-transaction* [dataset forms]
  `(let [dataset# (->jena-triple-store ~dataset)]
     (try
       (.begin dataset# ReadWrite/READ)
       ~@forms
       (finally
         (.end dataset#)))))

(defn- write-transaction* [dataset forms]
  `(let [dataset# (->jena-triple-store ~dataset)]
     (try
       (.begin ~dataset ReadWrite/WRITE)
       (let [result# ~@forms]
         (.commit ~dataset)
         result#)
       (finally
         (.end ~dataset)))))

(defmacro with-transaction [locktype dataset  & forms]
  (when-not (#{:read :write} locktype)
    (throw (IllegalArgumentException. "Transactions must be either :read or :write")))
  (if (= locktype :read)
    (read-transaction* dataset forms)
    (write-transaction* dataset forms)))

(defn import-file [dataset-assembly file graph]
  (let [dataset (TDBFactory/assembleDataset dataset-assembly)]
    (with-transaction :write dataset
      (let [model   (-> dataset (.getNamedModel graph))]
        (RDFDataMgr/read model file)
        (.close model)))
    (.close dataset)))


(defn- make-model-from-triples [dataset triple-source]
  "Creates a JENA model/graph from the supplied triples.  Attempts to
  resolve the triple-source String as either a filename or URI to load
  the triples from.  Doesn't name the graph of triples or explicitly
  add them to the dataset."
  (let [model (.getDefaultModel dataset)]
    (RDFDataMgr/read model triple-source)
    model))

(defn load-triples-into-graph [dataset graph-uri triple-source]
  "Load the triples from the specified triple-source (either a
  filename or a URI) into the given dataset and graph."
  (with-transaction :write dataset
    (let [model (make-model-from-triples dataset triple-source)]
      (.addNamedModel dataset graph-uri model))))


(defn exec-update [update-str graph-store]
  (let [update-req (UpdateFactory/create update-str)
        proc (UpdateExecutionFactory/create update-req graph-store)]
    (.execute proc)))


(def staging-base "http://publishmydata.com/id/drafter/graphs/")

(def drafter-state-graph "http://publishmydata.com/graphs/drafter-state")

(defn ->staging-graph [graph]
  (str staging-base (digest/sha1 graph)))

(def rdf:a "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
(def drafter:Graph "http://publishmydata.com/def/drafter/Graph")
(def drafter:hasGraph "http://publishmydata.com/def/drafter/hasGraph")

(defn create-basic-management-graph [graph-uri]
  (let [staging-graph (->staging-graph graph-uri)]

    [staging-graph [rdf:a drafter:Graph
                    drafter:hasGraph graph-uri]]))

(defn import-graph [db graph triples]
  (let [staging-graph (->staging-graph graph)]

    (with-transaction :write db
      (load-triples-into-graph db graph triples)

      )
    )

  ;; 1. generate a unique graph id for staging graph.
  ;; 2. Create graph in state staging add triples to it leave staging
  ;; graph in place so we don't need to take a copy of it to build
  ;; next version of staging graph.
  )

(defn migrate-graph [db graph]
  ;; 1. remove the destination graph
  ;; 2. lookup staging graph
  ;; 3. copy staging graph to "live graph name"
  ;; 4. leave staging graph in place for future staging changes
  )

(defn delete-graph [db graph]
  ;; 1. lookup staging graph
  ;; 2. remove staging graph
  )

(defn rename-graph [db old-graph new-graph]
  ;; lookup old-graph
  ;; calculate new graph sha
  ;; copy data/state to new graph name
  ;; remove old graph name
  ;; update subject name to new-graph uris in metadata graph.
  )



(comment
  (def db (TDBFactory/createDataset "/Users/rick/temp/drafter-dest-data3"))

  (with-transaction :read live (count (query live "SELECT * WHERE { ?s ?p ?o } LIMIT 10")))

)
