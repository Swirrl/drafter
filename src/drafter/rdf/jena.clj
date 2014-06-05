(ns drafter.rdf.jena
  (:import [com.hp.hpl.jena.tdb TDBFactory]
           [com.hp.hpl.jena.query
            QueryFactory QueryExecutionFactory
            QueryExecution Syntax Query ReadWrite
            ResultSetFormatter]
           [com.hp.hpl.jena.rdf.model ModelFactory]
           [org.apache.jena.riot Lang RDFDataMgr]))

(defn query [dataset query-str]
  (let [query (QueryFactory/create query-str)
        query-execution (QueryExecutionFactory/create query dataset)]

    (cond
     (.isAskType query) (-> query-execution .execAsk)
     (.isConstructType query) (-> query-execution .execConstructTriples iterator-seq)
     (.isDescribeType query) (-> query-execution .execDescribe)
     (.isSelectType query)  (-> query-execution .execSelect iterator-seq))))

(defn- read-transaction* [dataset forms]
  `(try
     (.begin ~dataset ReadWrite/READ)
     ~@forms
     (finally
       (.end ~dataset))))

(defn- write-transaction* [dataset forms]
  `(try
     (.begin ~dataset ReadWrite/WRITE)
     ~@forms
     (.commit ~dataset)
     (finally
       (.end ~dataset))))

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
    (def live (TDBFactory/createDataset "/Users/rick/temp/drafter-src-data3"))
    (def draft (TDBFactory/createDataset "/Users/rick/temp/drafter-dest-data3"))

    (with-transaction :read live (count (query live "SELECT * WHERE { ?s ?p ?o } LIMIT 10")))

    (def live (TDBFactory/createDataset "/tdb_data/digitalsocial_dev_data"))


    (comment
      ;; oldschool
      (def live (TDBFactory/assembleDataset "drafter-live.ttl"))
      (def draft (TDBFactory/assembleDataset "drafter-draft.ttl")))
    )
)
