(ns drafter.rdf.draft-management-test
  (:require
   [drafter.test-common :refer [*test-db* wrap-with-clean-test-db make-graph-live!]]
   [grafter.rdf :refer [s add add-statement]]
   [grafter.rdf.templater :refer [graph triplify]]
   [grafter.vocabularies.rdf :refer :all]
   [grafter.rdf.repository :refer :all]
   [grafter.rdf.protocols :refer [update!]]
   [drafter.rdf.draft-management :refer :all]
   [drafter.rdf.drafter-ontology :refer :all]
   [clojure.test :refer :all])
  (:import [org.openrdf.model.impl URIImpl]))

(defn ask? [& graphpatterns]
  "Bodgy convenience function for ask queries"
  (query *test-db* (str "ASK WHERE {"

                        (-> (apply str (interpose " " graphpatterns))
                            (.replace " >" ">")
                            (.replace "< " "<"))

                        "}")))

(defn clone-data-from-live-to-draft-query [draft-graph-uri]
  (str
   "INSERT {"
   "  GRAPH <" draft-graph-uri "> {"
   "    ?s ?p ?o ."
   "  }"
   "} WHERE { "
   (with-state-graph
     "?live <" rdf:a "> <" drafter:ManagedGraph "> ;"
     "      <" drafter:hasDraft "> <" draft-graph-uri "> .")
   "  GRAPH ?live {"
   "    ?s ?p ?o ."
   "  }"
   "}"))

(defn clone-data-from-live-to-draft!
  "Copy all of the data found in the drafts live graph into the
  specified draft."

  [repo draft-graph-uri]
  (update! repo (clone-data-from-live-to-draft-query draft-graph-uri)))

(defn clone-and-append-data! [db draft-graph-uri triples]
  (clone-data-from-live-to-draft! db draft-graph-uri)
  (append-data! db draft-graph-uri triples))

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
                               ["http://test2.com/hasProperty" "http://test2.com/data/2"]]))

(def test-graph-uri "http://example.org/my-graph")

(deftest create-managed-graph-test
  (testing "Creates managed graph"
    (is (create-managed-graph "http://example.org/my-graph"))))

(deftest create-managed-graph!-test
  (testing "create-managed-graph!"
    (testing "stores the details of the managed graph"
      (create-managed-graph! *test-db* test-graph-uri)
      (is (is-graph-managed? *test-db* test-graph-uri)))))

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
    (testing "on an empty live graph"
      (let [draft-graph-uri (create-managed-graph-with-draft! test-graph-uri)]

        (append-data! *test-db* draft-graph-uri test-triples)

        (is (ask? "GRAPH <" draft-graph-uri "> {
                 <http://test.com/data/one> <http://test.com/hasProperty> <http://test.com/data/1> .
               }"))))

    (testing "with live graph with existing data, copies data into draft"
      (let [live-uri (make-graph-live! *test-db*
                                       "http://clones/original/data"
                                       (triplify ["http://starting/data" ["http://starting/data" "http://starting/data"]]))
            draft-graph-uri (create-managed-graph-with-draft! live-uri)]

        (clone-and-append-data! *test-db* draft-graph-uri test-triples)

        (is (ask? "GRAPH <" draft-graph-uri "> {
                 <http://starting/data> <http://starting/data> <http://starting/data> .
                 <http://test.com/data/one> <http://test.com/hasProperty> <http://test.com/data/1> .
               }"))))))

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
  (let [draft-graph-uri (create-managed-graph-with-draft! test-graph-uri)
       expected-triple-pattern "<http://test.com/data/one> <http://test.com/hasProperty> <http://test.com/data/1> ."]
    (append-data! *test-db* draft-graph-uri test-triples)
    (migrate-live! *test-db* draft-graph-uri)
    (testing "migrate-live! data is migrated"
      (is (not (ask? "GRAPH <" draft-graph-uri "> {"
                     expected-triple-pattern
                     "}"))
          "Draft graph contents no longer exist.")

      (is (ask? "GRAPH <" test-graph-uri "> {"
                expected-triple-pattern
                "}")
          "Live graph contains the migrated triples")

      (is (= true (is-graph-managed? *test-db* test-graph-uri))
          "Live graph reference shouldn't have been deleted from state graph"))

 (let [my-test-graph-uri "http://example.org/my-other-graph"
       draft-graph-uri (create-managed-graph-with-draft! my-test-graph-uri)]
   (do
     ;; We are migrating empty graph, so this is deleting.
     (migrate-live! *test-db* draft-graph-uri)
     (let [managed-found? (is-graph-managed? *test-db* my-test-graph-uri)]
       (testing "migrate-live! DELETION: Deleted draft removes live graph from state graph"
         (is (not managed-found?)
             "Live graph should no longer be referenced in state graph")))))

    (let [my-test-graph-uri "http://example.org/my-other-graph"
          draft-graph-uri-1 (create-managed-graph-with-draft! my-test-graph-uri)
          _ (create-managed-graph-with-draft! my-test-graph-uri)]
      (do
        ;; We are migrating empty graph, so this is deleting.
        (migrate-live! *test-db* draft-graph-uri-1)
        (let [managed-found? (is-graph-managed? *test-db* my-test-graph-uri)]
          (testing "migrate-live! DELETION: Doesn't delete from state graph when there's multiple drafts"
            (is managed-found?
                "Live graph shouldn't be deleted from state graph if referenced by other drafts")))))))


(deftest graph-restricted-queries-test
  (testing "query"
    (testing "supports graph restriction"
      (add *test-db* "http://example.org/graph/1"
              test-triples)

      (add *test-db* "http://example.org/graph/2"
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

      (is (= false (query *test-db*
                          (str "ASK WHERE {
                           ?s ?p ?o .
                      }") :default-graph []))
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

(deftest draft-graphs-test
  (let [draft-1 (create-managed-graph-with-draft! "http://real/graph/1")
        draft-2 (create-managed-graph-with-draft! "http://real/graph/2")]

       (testing "draft-graphs returns all draft graphs"
         (is (= #{draft-1 draft-2} (draft-graphs *test-db*))))

       (testing "live-graphs returns all live graphs"
         (is (= #{"http://real/graph/1" "http://real/graph/2"}
                (live-graphs *test-db* :online false))))))

(deftest build-draft-map-test
  (let [db (repo)]
    (testing "graph-map associates live graphs with their drafts"
      (create-managed-graph! db "http://frogs.com/")
      (create-managed-graph! db "http://dogs.com/")

      (let [frogs-draft (create-draft-graph! db "http://frogs.com/")
            dogs-draft (create-draft-graph! db "http://dogs.com/")]

        (is (= {(URIImpl. "http://frogs.com/") (URIImpl. frogs-draft)
                (URIImpl. "http://dogs.com/")  (URIImpl. dogs-draft)}

               (graph-map db #{frogs-draft dogs-draft})))))))

(deftest upsert-single-object-insert-test
  (let [db (repo)]
    (upsert-single-object! db "http://foo/" "http://bar/" "baz")
    (is (query db "ASK { GRAPH <http://publishmydata.com/graphs/drafter/drafts> { <http://foo/> <http://bar/> \"baz\"} }"))))

(deftest upsert-single-object-update-test
  (let [db (repo)
        subject "http://example.com/subject"
        predicate "http://example.com/predicate"]
    (add db (triplify [subject [predicate (s "initial")]]))
    (upsert-single-object! db subject predicate "updated")
    (is (query db (str "ASK { GRAPH <http://publishmydata.com/graphs/drafter/drafts> {"
                       "<" subject "> <" predicate "> \"updated\""
                       "} }")))))

(use-fixtures :each wrap-with-clean-test-db)
