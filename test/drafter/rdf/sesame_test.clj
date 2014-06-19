(ns drafter.rdf.sesame-test
  (:require
   [grafter.rdf.protocols :as pr]
   [grafter.rdf :refer :all]
   [grafter.rdf.ontologies.rdf :refer :all]
   [grafter.rdf.sesame :refer :all]
   [drafter.rdf.sesame :refer :all]
   [drafter.rdf.drafter-ontology :refer :all]
   [clojure.test :refer :all]
   [me.raynes.fs :as fs]))

(def test-db-path "MyDatabases/test-db")

(def ^:dynamic *test-db* (repo (memory-store)))

(defn wrap-with-clean-test-db [f]
  (try
    (binding [*test-db* (repo (native-store test-db-path))]
      (f))
    (finally
        (fs/delete-dir test-db-path))))

(defn ask? [& graphpatterns]

  "Convenience function for ask queries"
  (query *test-db* (str "ASK WHERE {"

                        (-> (apply str (interpose " " graphpatterns))
                            (.replace " >" ">")
                            (.replace "< " "<"))

                        "}")))

(def test-triples (triplify ["http://test.com/data/one"
                             ["http://test.com/hasProperty" "http://test.com/data/1"]
                             ["http://test.com/hasProperty" "http://test.com/data/2"]]

                            ["http://test.com/data/two"
                             ["http://test.com/hasProperty" "http://test.com/data/1"]
                             ["http://test.com/hasProperty" "http://test.com/data/2"]]))

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

(deftest create-draft-graph!-test
  (testing "create-draft-graph!"

    (create-managed-graph! *test-db* test-graph-uri)

    (testing "returns the draft-graph-uri, stores data about it and associates it with the live graph"
      (let [new-graph-uri (create-draft-graph! *test-db*
                                               test-graph-uri)]

        (is (.startsWith new-graph-uri staging-base))
        (is (ask? "<" test-graph-uri "> <" rdf:a "> <" drafter:ManagedGraph "> ; "
                                       "<" drafter:hasDraft "> <" new-graph-uri "> ."))))))

(defn create-managed-graph-with-draft! [test-graph-uri]
  (create-managed-graph! *test-db* test-graph-uri)
  (create-draft-graph! *test-db* test-graph-uri))

(deftest append-data!-test
  (testing "append-data!"
    (let [draft-graph-uri (create-managed-graph-with-draft! test-graph-uri)]

      (append-data! *test-db* draft-graph-uri test-triples)

      (is (ask? "GRAPH <" draft-graph-uri "> {
                 <http://test.com/data/one> <http://test.com/hasProperty> <http://test.com/data/1> .
               }")))))

(deftest replace-data!-test
  (testing "replace-data!"
    (let [draft-graph-uri (create-managed-graph-with-draft! test-graph-uri)]

      (append-data! *test-db* draft-graph-uri (triplify ["http://removed/triple" ["http://is/" "http://gone/"]]))
      (replace-data! *test-db* draft-graph-uri test-triples)
      (is (not (ask? "<http://removed/triple>" "<http://is/>" "<http://gone/> .")))
      (is (ask? "<http://test.com/data/one> <http://test.com/hasProperty> <http://test.com/data/1> .")))))

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
