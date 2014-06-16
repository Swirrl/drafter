(ns drafter.rdf.jena-test
  (:import [com.hp.hpl.jena.tdb TDBFactory TDB]
           [com.hp.hpl.jena.query
            Dataset QueryFactory QueryExecutionFactory
            QueryExecution Syntax Query ReadWrite
            ResultSetFormatter ]
           [com.hp.hpl.jena.update GraphStore GraphStoreFactory UpdateExecutionFactory
            UpdateFactory UpdateProcessor UpdateRequest]
           [com.hp.hpl.jena.rdf.model ModelFactory]
           [org.apache.jena.riot Lang RDFDataMgr])
  (:require [clojure.test :refer :all]
            [drafter.rdf.jena :refer :all]
            [me.raynes.fs :as fs]))

(def test-db "MyDatabases/test-db")

(defn manage-test-db [f]
  (TDBFactory/createDataset test-db)
  (f)
  (fs/delete-dir test-db))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 1))))

(deftest import-graph
  (testing "Importing graph"

    (import-graph test-db "http://example.org/my-graph" "drafter-live.ttl")
    )
  )

(use-fixtures :each manage-test-db)
