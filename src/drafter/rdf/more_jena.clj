(ns drafter.rdf.more-jena
  (:require [drafter.rdf.jena :refer :all])
  (:import [com.hp.hpl.jena.tdb TDBFactory TDB]
           [com.hp.hpl.jena.query
            QueryFactory QueryExecutionFactory
            QueryExecution Syntax Query ReadWrite
            ResultSetFormatter]
           [com.hp.hpl.jena.update GraphStore GraphStoreFactory UpdateExecutionFactory
            UpdateFactory UpdateProcessor UpdateRequest]
           [com.hp.hpl.jena.rdf.model ModelFactory]
           [org.apache.jena.riot Lang RDFDataMgr]))

(def store-directory "MyDatabases/DB2")

(defn ex-tdb-txn2 [store-directory val]
  (let [dataset (TDBFactory/createDataset store-directory)]
    (.begin dataset ReadWrite/WRITE)
    (try
      (let [graph-store (GraphStoreFactory/create dataset)
            update-str (str "PREFIX : <http://example/>
                             INSERT DATA { :s :p3 \"foobar\" . }")]

        (let [update-req (UpdateFactory/create update-str)
              proc (UpdateExecutionFactory/create update-req graph-store)]
          (.execute proc))
        (.commit dataset)
        (println "commited"))
      (finally
        (println "ending")
        (.end dataset)))))


(defn create-dataset-with-data [ds]
  (let [dataset (->jena-triple-store ds)]
    (with-transaction :write dataset
      (let [graph-store (GraphStoreFactory/create dataset)
            update-str "PREFIX : <http://example/>
                        INSERT { :s6 :p ?now.
                                 :s7 :p ?now. } WHERE { BIND(now() AS ?now) }"
            update-str2 "PREFIX : <http://example/>
                        INSERT { :s6 :p ?now.
                                 :s9 :p ?now. } WHERE { BIND(now() AS ?now) }"]

        (exec-update update-str graph-store)
        (exec-update update-str2 graph-store)))
    (.close dataset)))


(comment
  (ex-tdb-txn2-2 store-directory)
  (def db1 (TDBFactory/createDataset store-directory)))
