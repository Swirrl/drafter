(ns drafter.rdf.draft-management-test
  (:require
   [drafter.test-common :refer [*test-backend* wrap-db-setup wrap-clean-test-db make-graph-live!]]
   [grafter.rdf :refer [s add add-statement]]
   [grafter.rdf.templater :refer [graph triplify]]
   [grafter.vocabularies.rdf :refer :all]
   [grafter.rdf.repository :refer :all]
   [drafter.backend.protocols :refer [append-data-batch!]]
   [drafter.backend.sesame.common.protocols :refer [->sesame-repo]]
   [grafter.rdf.protocols :refer [update!]]
   [drafter.rdf.draft-management :refer :all]
   [drafter.rdf.drafter-ontology :refer :all]
   [drafter.util :as util]
   [clojure.test :refer :all])
  (:import [java.util UUID]
           [org.openrdf.model.impl URIImpl]))

(defn ask? [& graphpatterns]
  "Bodgy convenience function for ask queries"
  (query *test-backend* (str "ASK WHERE {"

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
  (append-data-batch! db draft-graph-uri triples))

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
      (create-managed-graph! *test-backend* test-graph-uri)
      (is (is-graph-managed? *test-backend* test-graph-uri)))))

(deftest create-draftset!-test
  (let [title "Test draftset!"
        draftset-id (create-draftset! *test-backend* title)
        ds-uri (draftset-uri draftset-id)]
    (is (ask? "<" ds-uri "> <" rdf:a "> <" drafter:DraftSet ">"))
    (is (query *test-backend* (str "ASK WHERE { <" ds-uri "> <" rdfs:label "> \"" title "\" }")))
    (is (ask? "<" ds-uri "> <" drafter:createdAt "> ?o"))))

(deftest create-draft-graph!-test
  (testing "create-draft-graph!"

    (create-managed-graph! *test-backend* test-graph-uri)

    (testing "returns the draft-graph-uri, stores data about it and associates it with the live graph"
      (let [new-graph-uri (create-draft-graph! *test-backend*
                                               test-graph-uri)]

        (is (.startsWith new-graph-uri staging-base))
        (is (ask? "<" test-graph-uri "> <" rdf:a "> <" drafter:ManagedGraph "> ; "
                                       "<" drafter:hasDraft "> <" new-graph-uri "> .")))))

  (testing "within draft set"
    (create-managed-graph! *test-backend* test-graph-uri)
    (let [draftset-id (UUID/randomUUID)
          ds-uri (draftset-uri draftset-id)
          draft-graph-uri (create-draft-graph! *test-backend* test-graph-uri {} ds-uri)]
      (is (ask? "<" draft-graph-uri "> <" drafter:inDraftSet "> <" ds-uri ">")))))

(defn create-managed-graph-with-draft! [test-graph-uri]
  (create-managed-graph! *test-backend* test-graph-uri)
  (create-draft-graph! *test-backend* test-graph-uri))

(deftest append-data-batch!-test
  (testing "append-data-batch!"
    (testing "on an empty live graph"
      (let [draft-graph-uri (create-managed-graph-with-draft! test-graph-uri)]

        (append-data-batch! *test-backend* draft-graph-uri test-triples)

        (is (ask? "GRAPH <" draft-graph-uri "> {
                 <http://test.com/data/one> <http://test.com/hasProperty> <http://test.com/data/1> .
               }"))))

    (testing "with live graph with existing data, copies data into draft"
      (let [live-uri (make-graph-live! *test-backend*
                                       "http://clones/original/data"
                                       (triplify ["http://starting/data" ["http://starting/data" "http://starting/data"]]))
            draft-graph-uri (create-managed-graph-with-draft! live-uri)]

        (clone-and-append-data! *test-backend* draft-graph-uri test-triples)

        (is (ask? "GRAPH <" draft-graph-uri "> {
                 <http://starting/data> <http://starting/data> <http://starting/data> .
                 <http://test.com/data/one> <http://test.com/hasProperty> <http://test.com/data/1> .
               }"))))))

(deftest lookup-live-graph-test
  (testing "lookup-live-graph"
    (let [draft-graph-uri (create-managed-graph-with-draft! test-graph-uri)]
      (is (= test-graph-uri (lookup-live-graph *test-backend* draft-graph-uri))))))

(deftest set-isPublic!-test
  (testing "set-isPublic!"
    (create-managed-graph-with-draft! test-graph-uri)
    (is (ask? "<" test-graph-uri "> <" drafter:isPublic "> " false " .")
        "Graphs are initialsed as non-public")
    (set-isPublic! *test-backend* test-graph-uri true)
    (is (ask? "<" test-graph-uri "> <" drafter:isPublic "> " true " .")
        "should set the boolean value")))

(deftest migrate-live!-test
  (testing "migrate-live! data is migrated"
    (let [draft-graph-uri (create-managed-graph-with-draft! test-graph-uri)
          expected-triple-pattern "<http://test.com/data/one> <http://test.com/hasProperty> <http://test.com/data/1> ."]
      (append-data-batch! *test-backend* draft-graph-uri test-triples)
      (migrate-live! *test-backend* draft-graph-uri)
      (is (not (ask? "GRAPH <" draft-graph-uri "> {"
                     expected-triple-pattern
                     "}"))
          "Draft graph contents no longer exist.")

      (is (ask? "GRAPH <" test-graph-uri "> {"
                expected-triple-pattern
                "}")
          "Live graph contains the migrated triples")

      (is (= false (draft-exists? *test-backend* draft-graph-uri))
          "Draft graph should be removed from the state graph")

      (is (= true (is-graph-managed? *test-backend* test-graph-uri))
          "Live graph reference shouldn't have been deleted from state graph"))))

(deftest migrate-live!-remove-live-aswell-test
  (testing "migrate-live! DELETION: Deleted draft removes live graph from state graph"
    (let [test-graph-to-delete-uri "http://example.org/my-other-graph1"
          graph-to-keep-uri "http://example.org/keep-me-a1"
          graph-to-keep-uri2 "http://example.org/keep-me-a2"
          draft-graph-to-del-uri (create-managed-graph-with-draft!
                                  test-graph-to-delete-uri)
          draft-graph-to-keep-uri (create-managed-graph-with-draft! graph-to-keep-uri)
          draft-graph-to-keep-uri2 (create-managed-graph-with-draft! graph-to-keep-uri2)]

      (append-data-batch! *test-backend* draft-graph-to-keep-uri test-triples)
      (append-data-batch! *test-backend* draft-graph-to-keep-uri2 test-triples)
      (append-data-batch! *test-backend* draft-graph-to-del-uri test-triples)

      (migrate-live! *test-backend* draft-graph-to-keep-uri)
      (migrate-live! *test-backend* draft-graph-to-del-uri)

      ;; Draft for deletion has had data published. Now lets create a delete and publish
      (let [draft-graph-to-del-uri (create-managed-graph-with-draft! test-graph-to-delete-uri)]
        ;; We are migrating an empty graph, so this is deleting.
        (migrate-live! *test-backend* draft-graph-to-del-uri)
        (let [managed-found? (is-graph-managed? *test-backend* test-graph-to-delete-uri)
              keep-managed-found? (is-graph-managed? *test-backend* graph-to-keep-uri)]
          (is (not managed-found?)
              "Live graph should no longer be referenced in state graph")

          (is (= false (draft-exists? *test-backend* draft-graph-to-del-uri))
              "Draft graph should be removed from the state graph")
          (testing "Unrelated draft and live graphs should be not be removed from the state graph"
            (is keep-managed-found?)
            (is (draft-exists? *test-backend* draft-graph-to-keep-uri2))))))))

(deftest migrate-live!-dont-remove-state-when-other-drafts-test
  (testing "migrate-live! DELETION: Doesn't delete from state graph when there's multiple drafts"
    (let [test-graph-to-delete-uri "http://example.org/my-other-graph2"
          draft-graph-to-del-uri (create-managed-graph-with-draft! test-graph-to-delete-uri)
          _ (create-managed-graph-with-draft! test-graph-to-delete-uri)
          graph-to-keep-uri2 "http://example.org/keep-me-b2"
          graph-to-keep-uri3 "http://example.org/keep-me-b3"
          draft-graph-to-keep-uri2 (create-managed-graph-with-draft! graph-to-keep-uri2)
          draft-graph-to-keep-uri3 (create-managed-graph-with-draft! graph-to-keep-uri3)]

      (append-data-batch! *test-backend* draft-graph-to-keep-uri2 test-triples)
      (append-data-batch! *test-backend* draft-graph-to-keep-uri3 test-triples)
      (migrate-live! *test-backend* draft-graph-to-keep-uri2)
      (is (graph-exists? *test-backend* graph-to-keep-uri2))

      ;; We are migrating an empty graph, so this is deleting.
      (migrate-live! *test-backend* draft-graph-to-del-uri)
      (let [draft-managed-found? (is-graph-managed? *test-backend* test-graph-to-delete-uri)
            keep-managed-found? (is-graph-managed? *test-backend* graph-to-keep-uri2)]
        (is draft-managed-found?
            "Live graph shouldn't be deleted from state graph if referenced by other drafts")

        (is (= false (draft-exists? *test-backend* draft-graph-to-del-uri))
            "Draft graph should be removed from the state graph")

        (testing "Unrelated live & draft graphs should be not be removed from the state graph"
          (is keep-managed-found?)
          (is (draft-exists? *test-backend* draft-graph-to-keep-uri3)))))))

(deftest graph-restricted-queries-test
  (testing "query"
    (testing "supports graph restriction"
      (add *test-backend* "http://example.org/graph/1"
              test-triples)

      (add *test-backend* "http://example.org/graph/2"
              test-triples-2)

      (is (query *test-backend*
                 (str "ASK WHERE {
                         GRAPH <http://example.org/graph/2> {
                           ?s ?p ?o .
                         }
                      }") :named-graphs ["http://example.org/graph/2"])
          "Can query triples in named graph 2")

      (is (query *test-backend*
                 (str "ASK WHERE {
                         GRAPH <http://example.org/graph/1> {
                           <http://test.com/data/one>  ?p1 ?o1 .
                         }
                         GRAPH <http://example.org/graph/2> {
                           <http://test2.com/data/one> ?p2 ?o2 .
                         }
                      }") :named-graphs ["http://example.org/graph/1" "http://example.org/graph/2"])
          "Can specify many named graphs as a query restriction.")

      (is (= false (query *test-backend*
                          (str "ASK WHERE {
                                  GRAPH <http://example.org/graph/2> {
                                    ?s ?p ?o .
                                  }
                                }")
                          :named-graphs ["http://example.org/graph/1"]))
          "Can't query triples in named graph 2")

      (is (= false (query *test-backend*
                          (str "ASK WHERE {
                           ?s ?p ?o .
                      }") :default-graph []))
          "Can't query triples in union graph")

      (is (query *test-backend*
                 (str "ASK WHERE {
                           ?s ?p ?o .
                      }") :default-graph ["http://example.org/graph/1"])
          "Can query triples in union graph")

      (is (query *test-backend*
                 (str "ASK WHERE {
                           <http://test.com/data/one>  ?p1 ?o1 .
                           <http://test2.com/data/one> ?p2 ?o2 .
                      }") :default-graph ["http://example.org/graph/1" "http://example.org/graph/2"])
          "Can set many graphs as union graph"))))

(deftest draft-graphs-test
  (let [draft-1 (create-managed-graph-with-draft! "http://real/graph/1")
        draft-2 (create-managed-graph-with-draft! "http://real/graph/2")]

       (testing "draft-graphs returns all draft graphs"
         (is (= #{draft-1 draft-2} (draft-graphs *test-backend*))))

       (testing "live-graphs returns all live graphs"
         (is (= #{"http://real/graph/1" "http://real/graph/2"}
                (live-graphs *test-backend* :online false))))))

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

(deftest ensure-draft-graph-exists-for-test
  (testing "Draft graph already exists for live graph"
    (let [draft-graph-uri "http://draft"
          live-graph-uri  "http://live"
          ds-uri "http://draftset"
          initial-mapping {live-graph-uri draft-graph-uri}
          {found-draft-uri :draft-graph-uri graph-map :graph-map} (ensure-draft-exists-for *test-backend* live-graph-uri initial-mapping ds-uri)]
      (is (= draft-graph-uri found-draft-uri))
      (is (= initial-mapping graph-map))))

  (testing "Live graph exists without draft"
    (let [live-graph-uri (create-managed-graph! *test-backend* "http://live")
          ds-uri "http://draftset"
          {:keys [draft-graph-uri graph-map]} (ensure-draft-exists-for *test-backend* live-graph-uri {} ds-uri)]
      (is (= {live-graph-uri draft-graph-uri} graph-map))
      (is (is-graph-managed? *test-backend*  live-graph-uri))
      (is (draft-exists? *test-backend* draft-graph-uri))))

  (testing "Live graph does not exist"
    (let [live-graph-uri "http://live"
          ds-uri "http://draftset"
          {:keys [draft-graph-uri graph-map]} (ensure-draft-exists-for *test-backend* live-graph-uri {} ds-uri)]
      (is (= {live-graph-uri draft-graph-uri} graph-map))
      (is (is-graph-managed? *test-backend* live-graph-uri))
      (is (draft-exists? *test-backend* draft-graph-uri)))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
