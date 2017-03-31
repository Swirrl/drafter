(ns drafter.rdf.draft-management-test
  (:require
   [drafter.rdf.draftset-management :refer [create-draftset!]]
   [drafter.rdf.sparql :as sparql]
   [drafter.rdf.draft-management :refer :all]
   [drafter.test-helpers.draft-management-helpers :as mgmt]
   [drafter.user-test :refer [test-editor]]
   [drafter.test-common :refer [*test-backend* wrap-db-setup wrap-clean-test-db make-graph-live! import-data-to-draft! ask?] :as test]
   [grafter.rdf :refer [s add add-statement]]
   [grafter.rdf.templater :refer [graph triplify]]
   [grafter.vocabularies.rdf :refer :all]
   [grafter.rdf.repository :as repo]
   [drafter.rdf.drafter-ontology :refer :all]
   [schema.test :refer [validate-schemas]]
   [drafter.util :as util]
   [clojure.test :refer :all])
  (:import [org.openrdf.model.impl URIImpl]
           [java.util Date UUID]))

(use-fixtures :each validate-schemas)

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
  (sparql/update! repo (clone-data-from-live-to-draft-query draft-graph-uri)))

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

(deftest is-graph-live?-test
  (testing "Non-existent graph"
    (is (= false (is-graph-live? *test-backend* "http://missing"))))

  (testing "Non-live graph"
    (let [graph-uri (create-managed-graph! *test-backend* "http://live")]
      (is (= false (is-graph-live? *test-backend* graph-uri)))))

  (testing "Live graph"
    (let [graph-uri (make-graph-live! *test-backend* "http://live")]
      (is (is-graph-live? *test-backend* graph-uri)))))

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

(deftest migrate-graphs-to-live!-test
  (testing "migrate-graphs-to-live! data is migrated"
    (let [draft-graph-uri (create-managed-graph-with-draft! test-graph-uri)
          expected-triple-pattern "<http://test.com/data/one> <http://test.com/hasProperty> <http://test.com/data/1> ."]
      (append-data-batch! *test-backend* draft-graph-uri test-triples)
      (migrate-graphs-to-live! *test-backend* [draft-graph-uri])
      (is (not (ask? "GRAPH <" draft-graph-uri "> {"
                     expected-triple-pattern
                     "}"))
          "Draft graph contents no longer exist.")

      (is (ask? "GRAPH <" test-graph-uri "> {"
                expected-triple-pattern
                "}")
          "Live graph contains the migrated triples")

      (is (= false (mgmt/draft-exists? *test-backend* draft-graph-uri))
          "Draft graph should be removed from the state graph")

      (is (= true (is-graph-managed? *test-backend* test-graph-uri))
          "Live graph reference shouldn't have been deleted from state graph")

      (is (ask? "GRAPH <" drafter-state-graph "> {
                   <http://example.org/my-graph> dcterms:modified ?modified ;
                                                 dcterms:issued ?published .
                }")
          "Live graph should have a modified and issued time stamp"))))

(deftest migrate-graphs-to-live!-remove-live-aswell-test
  (testing "migrate-graphs-to-live! DELETION: Deleted draft removes live graph from state graph"
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

      (migrate-graphs-to-live! *test-backend* [draft-graph-to-keep-uri])
      (migrate-graphs-to-live! *test-backend* [draft-graph-to-del-uri])

      ;; Draft for deletion has had data published. Now lets create a delete and publish
      (let [draft-graph-to-del-uri (create-managed-graph-with-draft! test-graph-to-delete-uri)]
        ;; We are migrating an empty graph, so this is deleting.
        (migrate-graphs-to-live! *test-backend* [draft-graph-to-del-uri])
        (let [managed-found? (is-graph-managed? *test-backend* test-graph-to-delete-uri)
              keep-managed-found? (is-graph-managed? *test-backend* graph-to-keep-uri)]
          (is (not managed-found?)
              "Live graph should no longer be referenced in state graph")

          (is (= false (mgmt/draft-exists? *test-backend* draft-graph-to-del-uri))
              "Draft graph should be removed from the state graph")
          (testing "Unrelated draft and live graphs should be not be removed from the state graph"
            (is keep-managed-found?)
            (is (mgmt/draft-exists? *test-backend* draft-graph-to-keep-uri2))))))))

(deftest migrate-graphs-to-live!-dont-remove-state-when-other-drafts-test
  (testing "migrate-graphs-to-live! DELETION: Doesn't delete from state graph when there's multiple drafts"
    (let [test-graph-to-delete-uri "http://example.org/my-other-graph2"
          draft-graph-to-del-uri (create-managed-graph-with-draft! test-graph-to-delete-uri)
          _ (create-managed-graph-with-draft! test-graph-to-delete-uri)
          graph-to-keep-uri2 "http://example.org/keep-me-b2"
          graph-to-keep-uri3 "http://example.org/keep-me-b3"
          draft-graph-to-keep-uri2 (create-managed-graph-with-draft! graph-to-keep-uri2)
          draft-graph-to-keep-uri3 (create-managed-graph-with-draft! graph-to-keep-uri3)]

      (append-data-batch! *test-backend* draft-graph-to-keep-uri2 test-triples)
      (append-data-batch! *test-backend* draft-graph-to-keep-uri3 test-triples)
      (migrate-graphs-to-live! *test-backend* [draft-graph-to-keep-uri2])
      (is (graph-exists? *test-backend* graph-to-keep-uri2))

      ;; We are migrating an empty graph, so this is deleting.
      (migrate-graphs-to-live! *test-backend* [draft-graph-to-del-uri])
      (let [draft-managed-found? (is-graph-managed? *test-backend* test-graph-to-delete-uri)
            keep-managed-found? (is-graph-managed? *test-backend* graph-to-keep-uri2)]
        (is draft-managed-found?
            "Live graph shouldn't be deleted from state graph if referenced by other drafts")

        (is (= false (mgmt/draft-exists? *test-backend* draft-graph-to-del-uri))
            "Draft graph should be removed from the state graph")

        (testing "Unrelated live & draft graphs should be not be removed from the state graph"
          (is keep-managed-found?)
          (is (mgmt/draft-exists? *test-backend* draft-graph-to-keep-uri3)))))))

(deftest graph-restricted-queries-test
  (testing "query"
    (testing "supports graph restriction"
      (add *test-backend* "http://example.org/graph/1"
              test-triples)

      (add *test-backend* "http://example.org/graph/2"
              test-triples-2)

      (is (sparql/query *test-backend*
                 (str "ASK WHERE {
                         GRAPH <http://example.org/graph/2> {
                           ?s ?p ?o .
                         }
                      }") :named-graphs ["http://example.org/graph/2"])
          "Can query triples in named graph 2")

      (is (sparql/query *test-backend*
                 (str "ASK WHERE {
                         GRAPH <http://example.org/graph/1> {
                           <http://test.com/data/one>  ?p1 ?o1 .
                         }
                         GRAPH <http://example.org/graph/2> {
                           <http://test2.com/data/one> ?p2 ?o2 .
                         }
                      }") :named-graphs ["http://example.org/graph/1" "http://example.org/graph/2"])
          "Can specify many named graphs as a query restriction.")

      (is (= false (sparql/query *test-backend*
                          (str "ASK WHERE {
                                  GRAPH <http://example.org/graph/2> {
                                    ?s ?p ?o .
                                  }
                                }")
                          :named-graphs ["http://example.org/graph/1"]))
          "Can't query triples in named graph 2")

      (is (= false (sparql/query *test-backend*
                          (str "ASK WHERE {
                           ?s ?p ?o .
                      }") :default-graph []))
          "Can't query triples in union graph")

      (is (sparql/query *test-backend*
                 (str "ASK WHERE {
                           ?s ?p ?o .
                      }") :default-graph ["http://example.org/graph/1"])
          "Can query triples in union graph")

      (is (sparql/query *test-backend*
                 (str "ASK WHERE {
                           <http://test.com/data/one>  ?p1 ?o1 .
                           <http://test2.com/data/one> ?p2 ?o2 .
                      }") :default-graph ["http://example.org/graph/1" "http://example.org/graph/2"])
          "Can set many graphs as union graph"))))

(deftest draft-graphs-test
  (let [draft-1 (create-managed-graph-with-draft! "http://real/graph/1")
        draft-2 (create-managed-graph-with-draft! "http://real/graph/2")]

       (testing "draft-graphs returns all draft graphs"
         (is (= #{draft-1 draft-2} (mgmt/draft-graphs *test-backend*))))

       (testing "live-graphs returns all live graphs"
         (is (= #{"http://real/graph/1" "http://real/graph/2"}
                (live-graphs *test-backend* :online false))))))

(deftest build-draft-map-test
  (let [db (repo/repo)]
    (testing "graph-map associates live graphs with their drafts"
      (create-managed-graph! db "http://frogs.com/")
      (create-managed-graph! db "http://dogs.com/")
      (let [frogs-draft (create-draft-graph! db "http://frogs.com/")
            dogs-draft (create-draft-graph! db "http://dogs.com/")]

        (is (= {(URIImpl. "http://frogs.com/") (URIImpl. frogs-draft)
                (URIImpl. "http://dogs.com/")  (URIImpl. dogs-draft)}

               (graph-map db #{frogs-draft dogs-draft})))))))

(deftest upsert-single-object-insert-test
  (let [db (repo/repo)]
    (upsert-single-object! db "http://foo/" "http://bar/" "baz")
    (is (sparql/query db "ASK { GRAPH <http://publishmydata.com/graphs/drafter/drafts> { <http://foo/> <http://bar/> \"baz\"} }"))))

(deftest upsert-single-object-update-test
  (let [db (repo/repo)
        subject "http://example.com/subject"
        predicate "http://example.com/predicate"]
    (add db (triplify [subject [predicate (s "initial")]]))
    (upsert-single-object! db subject predicate "updated")
    (is (sparql/query db (str "ASK { GRAPH <http://publishmydata.com/graphs/drafter/drafts> {"
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
      (is (mgmt/draft-exists? *test-backend* draft-graph-uri))))

  (testing "Live graph does not exist"
    (let [live-graph-uri "http://live"
          ds-uri "http://draftset"
          {:keys [draft-graph-uri graph-map]} (ensure-draft-exists-for *test-backend* live-graph-uri {} ds-uri)]
      (is (= {live-graph-uri draft-graph-uri} graph-map))
      (is (is-graph-managed? *test-backend* live-graph-uri))
      (is (mgmt/draft-exists? *test-backend* draft-graph-uri)))))

(deftest delete-draft-graph!-test
  (testing "With only draft for managed graph"
    (let [live-graph-uri (create-managed-graph! *test-backend* "http://live")
          draft-graph-uri (create-draft-graph! *test-backend* live-graph-uri)]
      (append-data-batch! *test-backend* draft-graph-uri test-triples)
      (delete-draft-graph! *test-backend* draft-graph-uri)

      (testing "should delete graph data"
        (is (= false (ask? "GRAPH <" draft-graph-uri "> { ?s ?p ?o }"))))

      (testing "should delete graph from state"
        (is (= false (ask? (with-state-graph "<" draft-graph-uri "> ?p ?o")))))

      (testing "should delete managed graph"
        (is (= false (is-graph-managed? *test-backend* live-graph-uri))))))

  (testing "Draft for managed graph with other graphs"
    (let [live-graph-uri (create-managed-graph! *test-backend* "http://live")
          draft-graph-1 (create-draft-graph! *test-backend* live-graph-uri)
          draft-graph-2 (create-draft-graph! *test-backend* live-graph-uri)]

      (delete-draft-graph! *test-backend* draft-graph-2)

      (is (= true (mgmt/draft-exists? *test-backend* draft-graph-1)))
      (is (= true (is-graph-managed? *test-backend* live-graph-uri))))))

;; This test attempts to capture the rationale behind the calculation of graph
;; restrictions.
;;
;; These tests attempt to recreate the various permutations of what will happen
;; when union-with-live=true/false and when there are drafts specified or not.
(deftest calculate-graph-restriction-test
  (testing "union-with-live=true"
    (testing "with no drafts specified"
      (is (= #{:l1 :l2}
             (calculate-graph-restriction #{:l1 :l2} #{} #{}))
          "Restriction should be the set of public live graphs"))

    (testing "with drafts specified"
      (is (= #{:l1 :d2 :d3 :d4}
             (calculate-graph-restriction #{:l1 :l2} #{:l3 :l4 :l2} #{:d2 :d3 :d4}))
          "Restriction should be the set of live graphs plus drafts from the
            draftset.  Live graphs for the specified drafts should not be
            included as we want to use their draft graphs.")))

  (testing "union-with-live=false"
    (testing "with no drafts specified"
      (is (= #{}
             (calculate-graph-restriction #{} #{} #{}))
          "Restriction should be the set of public live graphs"))
    (testing "with drafts specified"
      (is (= #{:d1 :d2}
             (calculate-graph-restriction #{} #{:l1 :l2} #{:d1 :d2}))
          "Restriction should be the set of drafts (as live graph queries will be rewritten to their draft graphs)"))))

(deftest set-timestamp-test
  (let [draftset (create-draftset! *test-backend* test-editor)
        triples (test/test-triples "http://test-subject")
        draft-graph-uri (import-data-to-draft! *test-backend* "http://foo/graph" triples draftset)]

    (set-modifed-at-on-draft-graph! *test-backend* draft-graph-uri (Date.))

    (is (sparql/query *test-backend*
               (str
                "ASK {"
                "<" draft-graph-uri "> <" drafter:modifiedAt "> ?modified . "
                "}")))))

(defn test-quads [g]
  (map #(assoc % :c g)
       (test/test-triples "http://test-subject")))

(deftest copy-graph-test
  (let [repo (repo/repo)]
    (add repo (test-quads "http://test-graph/1"))

    (copy-graph repo "http://test-graph/1" "http://test-graph/2")

    (let [source-graph (set (sparql/query repo "SELECT * WHERE { GRAPH <http://test-graph/1> { ?s ?p ?o }}"))
          dest-graph   (set (sparql/query repo "SELECT * WHERE { GRAPH <http://test-graph/2> { ?s ?p ?o }}"))]

      (is (not (empty? dest-graph))
          "Should not be empty (and have the data we loaded)")
      (is (= source-graph
             dest-graph)
          "Should be a copy of the source graph"))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
