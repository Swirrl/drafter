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

(def test-db-path "MyDatabases/repositories/test-db")

(def ^:dynamic *test-db* (repo (memory-store)))

(defn wrap-with-clean-test-db [f]
  (try
    (binding [*test-db* (repo (native-store test-db-path))]
      (f))
    (finally
        (fs/delete-dir test-db-path))))

(defn ask? [& graphpatterns]

  "Bodgy convenience function for ask queries"
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

(def test-triples-2 (triplify ["http://test2.com/data/one"
                             ["http://test2.com/hasProperty" "http://test2.com/data/1"]
                             ["http://test2.com/hasProperty" "http://test2.com/data/2"]]

                            ["http://test2.com/data/two"
                             ["http://test2.com/hasProperty" "http://test2.com/data/1"]
                             ["http://test2.com/hasProperty" "http://test2.com/data/2"]])
  )

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

(deftest lookup-live-graph-test
  (testing "lookup-live-graph"
    (let [draft-graph-uri (create-managed-graph-with-draft! test-graph-uri)]
      (is (= test-graph-uri (lookup-live-graph *test-db* draft-graph-uri))))))

(deftest set-isPublic!-test
  (testing "set-isPublic!"
    (create-managed-graph-with-draft! test-graph-uri)
    (is (ask? "<" test-graph-uri "> <" drafter:isPublic "> " false " .")
        "Graphs are initialsed as non-public")
    (set-isPublic! *test-db* test-graph-uri true)
    (is (ask? "<" test-graph-uri "> <" drafter:isPublic "> " true " .")
        "should set the boolean value")))

(deftest migrate-live!-test
  (testing "migrate-live!"
    (let [draft-graph-uri (create-managed-graph-with-draft! test-graph-uri)
          expected-triple-pattern "<http://test.com/data/one> <http://test.com/hasProperty> <http://test.com/data/1> ."]
      (append-data! *test-db* draft-graph-uri test-triples)
      (migrate-live! *test-db* draft-graph-uri)

      (is (not (ask? "GRAPH <" draft-graph-uri "> {"
                     expected-triple-pattern
                     "}"))
          "Draft graph no longer exists.")

      (is (ask? "GRAPH <" test-graph-uri "> {"
                expected-triple-pattern
                "}")
          "Live graph contains the migrated triples"))))

(deftest graph-restricted-queries-test
  (testing "query"
    (testing "supports graph restriction"
      (pr/add *test-db* "http://example.org/graph/1"
              test-triples)

      (pr/add *test-db* "http://example.org/graph/2"
              test-triples-2)

      (is (query *test-db*
                 (str "ASK WHERE {
                         GRAPH <http://example.org/graph/2> {
                           ?s ?p ?o .
                         }
                      }") :named-graphs ["http://example.org/graph/2"])
          "Can query triples in named graph 2")

      (is (query *test-db*
                 (str "ASK WHERE {
                         GRAPH <http://example.org/graph/1> {
                           <http://test.com/data/one>  ?p1 ?o1 .
                         }
                         GRAPH <http://example.org/graph/2> {
                           <http://test2.com/data/one> ?p2 ?o2 .
                         }
                      }") :named-graphs ["http://example.org/graph/1" "http://example.org/graph/2"])
          "Can specify many named graphs as a query restriction.")

      (is (= false (query *test-db*
                          (str "ASK WHERE {
                                  GRAPH <http://example.org/graph/2> {
                                    ?s ?p ?o .
                                  }
                                }")
                          :named-graphs ["http://example.org/graph/1"]))
          "Can't query triples in named graph 2")

      (is (query *test-db*
                 (str "ASK WHERE {
                           ?s ?p ?o .
                      }") :default-graph [])
          "Can't query triples in union graph")

      (is (query *test-db*
                 (str "ASK WHERE {
                           ?s ?p ?o .
                      }") :default-graph ["http://example.org/graph/1"])
          "Can query triples in union graph")

      (is (query *test-db*
                 (str "ASK WHERE {
                           <http://test.com/data/one>  ?p1 ?o1 .
                           <http://test2.com/data/one> ?p2 ?o2 .
                      }") :default-graph ["http://example.org/graph/1" "http://example.org/graph/2"])
          "Can set many graphs as union graph"))))

(comment

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

(comment
  (pr/add *test-db* (rdf-serializer "test.ttl"))

  (pr/add *test-db* (graph "http://foo.com/" ["http://foo.com/" ["http://foo.com/" "http://foo.com/"]]))

  )
