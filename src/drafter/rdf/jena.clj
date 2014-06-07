(ns drafter.rdf.jena
  (:import [com.hp.hpl.jena.tdb TDBFactory TDB]
           [com.hp.hpl.jena.query
            Dataset QueryFactory QueryExecutionFactory
            QueryExecution Syntax Query ReadWrite
            ResultSetFormatter ]
           [com.hp.hpl.jena.update GraphStore GraphStoreFactory UpdateExecutionFactory
            UpdateFactory UpdateProcessor UpdateRequest]
           [com.hp.hpl.jena.rdf.model ModelFactory]
           [org.apache.jena.riot Lang RDFDataMgr]))

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
       ~@forms
       (.commit ~dataset)
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

(defn migrate [from to]
  (with-transaction :read from
    (let [from-graphs (iterator-seq (.listNames from))]

      (doseq [src-graph from-graphs]
        (println src-graph)
        (with-transaction :write to
          (let [src-model  (-> from (.getNamedModel src-graph))
                dest-model (-> to (.getNamedModel src-graph))
                statements (.listStatements src-model)]

            (println "done list")
            (.add dest-model statements)))))))

(defn exec-update [update-str graph-store]
  (let [update-req (UpdateFactory/create update-str)
        proc (UpdateExecutionFactory/create update-req graph-store)]
    (.execute proc)))

(comment

  (let [dataset (TDBFactory/assembleDataset "drafter-live.ttl")]

    (.begin dataset ReadWrite/READ)

    (let [model (.getNamedModel dataset "http://data.digitalsocial.eu/graph/organizations-and-activities")
          statements (take 10 (iterator-seq (.listStatements model)))]
      (.close dataset)

      statements))


  )

(comment
  (do
    (def live (TDBFactory/createDataset "/Users/rick/temp/drafterdestdata"))
    (def draft (TDBFactory/createDataset "/Users/rick/temp/drafter-dest-data3"))

    (with-transaction :read live (count (query live "SELECT * WHERE { ?s ?p ?o } LIMIT 10")))

    (def live (TDBFactory/createDataset "/tdb_data/digitalsocial_dev_data"))


    (comment
      ;; oldschool
      (def live (TDBFactory/assembleDataset "drafter-live.ttl"))
      (def draft (TDBFactory/assembleDataset "drafter-draft.ttl")))
    )
)
