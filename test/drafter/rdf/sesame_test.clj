(ns drafter.rdf.sesame-test
  (:require
   [grafter.rdf.protocols :as pr]
   [grafter.rdf :refer :all]
   [grafter.rdf.ontologies.rdf :refer :all]
   [grafter.rdf.sesame :refer :all]
   [drafter.rdf.sesame :refer :all]
   [clojure.test :refer :all]
   [me.raynes.fs :as fs]))

(def test-db-path "MyDatabases/test-db")

(def ^:dynamic *test-db* nil)

(defn wrap-with-clean-test-db [f]
  (try
    (binding [*test-db* (repo (native-store test-db-path))]
      (f))
    (finally
        (fs/delete-dir test-db-path))))

(def test-triples (triplify ["http://test.com/data/one"
                             ["http://test.com/hasItem" "http://test.com/data/1"]
                             ["http://test.com/hasItem" "http://test.com/data/2"]]

                            ["http://test.com/data/two"
                             ["http://test.com/hasItem" "http://test.com/data/1"]
                             ["http://test.com/hasItem" "http://test.com/data/2"]]))

(def test-graph-uri "http://example.org/my-graph")

(deftest create-managed-graph-test
  (testing "Creates managed graph"
    (is (create-managed-graph "http://example.org/my-graph"))))

(deftest create-managed-graph!-test
  (testing "create-managed-graph!"
    (testing "stores the details of the managed graph"
      (create-managed-graph! *test-db* test-graph-uri)
      (is (is-graph-managed? *test-db* test-graph-uri)))

    (testing "protects against graphs being created twice"
      (is (thrown? clojure.lang.ExceptionInfo
                   (create-managed-graph! *test-db* test-graph-uri))))))


(comment

  (deftest make-managed-graph
    (testing "Making managed graph"
      (testing "stores live graph data"
        (let [staging-graph-uri (make-draft *test-db* test-graph-uri)]
          (is (query *test-db*
                     (str "ASK WHERE {
                           <" staging-graph-uri "> <http://publishmydata.com/def/drafter/hasGraph> <" test-graph-uri "> .
                        }")))))))

  (deftest import-graph-test
           (testing "Importing graph"
             (testing "Creates a new state graph"
               (import-graph *test-db* test-graph-uri  test-triples)
               (is (has-management-graph? *test-db* test-graph-uri))

               (testing "which is in a draft state")
               (testing "which is named after the SHA1 of the graph name"))
             (testing "Stores the raw data in a staging graph")

             ;;(import-graph test-db "http://example.org/my-graph" "drafter-live.ttl")
             )))

(use-fixtures :each wrap-with-clean-test-db)
