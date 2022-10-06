(ns drafter.backend.draftset.draft-management-test
  (:require [clojure.test :refer :all :as t]
            [drafter.backend.draftset.draft-management :refer :all]
            [drafter.backend.draftset.operations :refer [create-draftset!]]
            [drafter.draftset :as ds]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.sparql :as sparql]
            [drafter.test-common
             :as
             test
             :refer
             [*test-backend*
              ask?
              make-graph-live!
              wrap-system-setup]]
            [drafter.test-helpers.draft-management-helpers :as mgmt]
            [drafter.user-test :refer [test-editor]]
            [grafter-2.rdf4j.templater :refer [triplify]]
            [grafter-2.rdf4j.repository :as repo]
            [grafter.vocabularies.dcterms :refer [dcterms:issued]]
            [grafter.vocabularies.rdf :refer :all]
            [schema.test :refer [validate-schemas]]
            [drafter.util :as util]
            [drafter.test-common :as tc]
            [drafter.backend.draftset.graphs :as graphs]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.time :as time]
            [grafter-2.rdf.protocols :as pr])
  (:import java.net.URI))

(use-fixtures :each validate-schemas tc/with-spec-instrumentation)

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
  "Copy all the data found in the drafts live graph into the specified draft."
  [repo draft-graph-uri]
  (sparql/update! repo (clone-data-from-live-to-draft-query draft-graph-uri)))

(defn clone-and-append-data! [db draft-graph-uri triples]
  (clone-data-from-live-to-draft! db draft-graph-uri)
  (append-data-batch! db draft-graph-uri triples))

(def test-triples (triplify [(URI. "http://test.com/data/one")
                             [(URI. "http://test.com/hasProperty") (URI. "http://test.com/data/1")]
                             [(URI. "http://test.com/hasProperty") (URI. "http://test.com/data/2")]]

                            [(URI. "http://test.com/data/two")
                             [(URI. "http://test.com/hasProperty") (URI. "http://test.com/data/1")]
                             [(URI. "http://test.com/hasProperty") (URI. "http://test.com/data/2")]]))

(def test-triples-2 (triplify [(URI. "http://test2.com/data/one")
                               [(URI. "http://test2.com/hasProperty") (URI. "http://test2.com/data/1")]
                               [(URI. "http://test2.com/hasProperty") (URI. "http://test2.com/data/2")]]

                              [(URI. "http://test2.com/data/two")
                               [(URI. "http://test2.com/hasProperty") (URI. "http://test2.com/data/1")]
                               [(URI. "http://test2.com/hasProperty") (URI. "http://test2.com/data/2")]]))

(def test-graph-uri (URI. "http://example.org/my-graph"))

(defn- create-managed-graph! [db graph-uri]
  (let [manager (graphs/create-manager db)]
    (graphs/ensure-managed-user-graph manager graph-uri)))

(defn- create-managed-graph-with-draft! [test-graph-uri]
  (let [manager (graphs/create-manager *test-backend*)
        draftset-id (dsops/create-draftset! *test-backend* test-editor)]
    (graphs/create-user-graph-draft manager draftset-id test-graph-uri)))

(deftest is-graph-live?-test
  (testing "Non-existent graph"
    (is (= false (is-graph-live? *test-backend* (URI. "http://missing")))))

  (testing "Non-live graph"
    (let [graph-uri (create-managed-graph! *test-backend* (URI. "http://live"))]
      (is (= false (is-graph-live? *test-backend* graph-uri)))))

  (testing "Live graph"
    (let [graph-uri (make-graph-live! *test-backend* (URI. "http://live"))]
      (is (is-graph-live? *test-backend* graph-uri)))))

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
                                       (URI. "http://clones/original/data")
                                       (triplify [(URI. "http://starting/data") [(URI. "http://starting/data") (URI. "http://starting/data")]]))
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
      (migrate-graphs-to-live! *test-backend* [draft-graph-uri] (time/parse "2015-01-01T00:00:00Z"))
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

      (is (ask? "GRAPH <" drafter-state-graph "> {"
                "  <http://example.org/my-graph> <" dcterms:issued "> ?published ."
                "}")
          "Live graph should have a modified and issued time stamp"))))

(deftest migrate-graphs-to-live!-remove-live-aswell-test
  (testing "migrate-graphs-to-live! DELETION: Deleted draft removes live graph from state graph"
    (let [test-graph-to-delete-uri (URI. "http://example.org/my-other-graph1")
          graph-to-keep-uri (URI. "http://example.org/keep-me-a1")
          graph-to-keep-uri2 (URI. "http://example.org/keep-me-a2")
          draft-graph-to-del-uri (create-managed-graph-with-draft!
                                  test-graph-to-delete-uri)
          draft-graph-to-keep-uri (create-managed-graph-with-draft! graph-to-keep-uri)
          draft-graph-to-keep-uri2 (create-managed-graph-with-draft! graph-to-keep-uri2)]

      (append-data-batch! *test-backend* draft-graph-to-keep-uri test-triples)
      (append-data-batch! *test-backend* draft-graph-to-keep-uri2 test-triples)
      (append-data-batch! *test-backend* draft-graph-to-del-uri test-triples)

      (migrate-graphs-to-live! *test-backend* [draft-graph-to-keep-uri] (time/parse "2015-01-01T00:00:00Z"))
      (migrate-graphs-to-live! *test-backend* [draft-graph-to-del-uri] (time/parse "2016-01-01T00:00:00Z"))

      ;; Draft for deletion has had data published. Now lets create a delete and publish
      (let [draft-graph-to-del-uri (create-managed-graph-with-draft! test-graph-to-delete-uri)]
        ;; We are migrating an empty graph, so this is deleting.
        (migrate-graphs-to-live! *test-backend* [draft-graph-to-del-uri] (time/parse "2017-01-01T00:00:00Z"))
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
    (let [test-graph-to-delete-uri (URI. "http://example.org/my-other-graph2")
          draft-graph-to-del-uri (create-managed-graph-with-draft! test-graph-to-delete-uri)
          _ (create-managed-graph-with-draft! test-graph-to-delete-uri)
          graph-to-keep-uri2 (URI. "http://example.org/keep-me-b2")
          graph-to-keep-uri3 (URI. "http://example.org/keep-me-b3")
          draft-graph-to-keep-uri2 (create-managed-graph-with-draft! graph-to-keep-uri2)
          draft-graph-to-keep-uri3 (create-managed-graph-with-draft! graph-to-keep-uri3)]

      (append-data-batch! *test-backend* draft-graph-to-keep-uri2 test-triples)
      (append-data-batch! *test-backend* draft-graph-to-keep-uri3 test-triples)
      (migrate-graphs-to-live! *test-backend* [draft-graph-to-keep-uri2] (time/parse "2015-01-01T00:00:00Z"))
      (is (graph-non-empty? *test-backend* graph-to-keep-uri2))

      ;; We are migrating an empty graph, so this is deleting.
      (migrate-graphs-to-live! *test-backend* [draft-graph-to-del-uri] (time/parse "2016-01-01T00:00:00Z"))
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
      (sparql/add *test-backend* "http://example.org/graph/1"
              test-triples)

      (sparql/add *test-backend* "http://example.org/graph/2"
              test-triples-2)

      (is (sparql/eager-query *test-backend*
                 (str "ASK WHERE {
                         GRAPH <http://example.org/graph/2> {
                           ?s ?p ?o .
                         }
                      }") {:named-graphs ["http://example.org/graph/2"]})
          "Can query triples in named graph 2")

      (is (sparql/eager-query *test-backend*
                 (str "ASK WHERE {
                         GRAPH <http://example.org/graph/1> {
                           <http://test.com/data/one>  ?p1 ?o1 .
                         }
                         GRAPH <http://example.org/graph/2> {
                           <http://test2.com/data/one> ?p2 ?o2 .
                         }
                      }") {:named-graphs ["http://example.org/graph/1" "http://example.org/graph/2"]})
          "Can specify many named graphs as a query restriction.")

      (is (= false (sparql/eager-query *test-backend*
                          (str "ASK WHERE {
                                  GRAPH <http://example.org/graph/2> {
                                    ?s ?p ?o .
                                  }
                                }")
                          {:named-graphs ["http://example.org/graph/1"]}))
          "Can't query triples in named graph 2")

      (is (= false (sparql/eager-query *test-backend*
                          (str "ASK WHERE {
                           ?s ?p ?o .
                      }") {:default-graph []}))
          "Can't query triples in union graph")

      (is (sparql/eager-query *test-backend*
                 (str "ASK WHERE {
                           ?s ?p ?o .
                      }") {:default-graph ["http://example.org/graph/1"]})
          "Can query triples in union graph")

      (is (sparql/eager-query *test-backend*
                 (str "ASK WHERE {
                           <http://test.com/data/one>  ?p1 ?o1 .
                           <http://test2.com/data/one> ?p2 ?o2 .
                      }") {:default-graph ["http://example.org/graph/1" "http://example.org/graph/2"]})
          "Can set many graphs as union graph"))))

(deftest draft-graphs-test
  (let [live-1 (URI. "http://real/graph/1")
        live-2 (URI. "http://real/graph/2")
        draft-1 (create-managed-graph-with-draft! live-1)
        draft-2 (create-managed-graph-with-draft! live-2)]

       (testing "draft-graphs returns all draft graphs"
         (is (= #{draft-1 draft-2} (mgmt/draft-graphs *test-backend*))))

       (testing "live-graphs returns all live graphs"
         (is (= #{live-1 live-2}
                (live-graphs *test-backend* :online false))))))

(deftest upsert-single-object-insert-test
  (let [db (repo/sail-repo)]
    (upsert-single-object! db (URI. "http://foo/") (URI. "http://bar/") "baz")
    (is (sparql/eager-query db "ASK { GRAPH <http://publishmydata.com/graphs/drafter/drafts> { <http://foo/> <http://bar/> \"baz\"} }"))))

(deftest upsert-single-object-update-test
  (let [db (repo/sail-repo)
        subject (URI. "http://example.com/subject")
        predicate (URI. "http://example.com/predicate")]
    (sparql/add db (triplify [subject [predicate "initial"]]))
    (upsert-single-object! db subject predicate "updated")
    (is (sparql/eager-query db (str "ASK { GRAPH <http://publishmydata.com/graphs/drafter/drafts> {"
                       "<" subject "> <" predicate "> \"updated\""
                       "} }")))))

(deftest delete-draft-graph!-test
  (testing "With only draft for managed graph"
    (let [live-graph-uri (URI. "http://live")
          draft-graph-uri (create-managed-graph-with-draft! live-graph-uri)]
      (append-data-batch! *test-backend* draft-graph-uri test-triples)
      (delete-draft-graph! *test-backend* draft-graph-uri)

      (testing "should delete graph data"
        (is (= false (ask? "GRAPH <" draft-graph-uri "> { ?s ?p ?o }"))))

      (testing "should delete graph from state"
        (is (= false (ask? (with-state-graph "<" draft-graph-uri "> ?p ?o")))))

      (testing "should delete managed graph"
        (is (= false (is-graph-managed? *test-backend* live-graph-uri))))))

  (testing "Draft for managed graph with other graphs"
    (let [live-graph-uri (URI. "http://live")
          draft-graph-1 (create-managed-graph-with-draft! live-graph-uri)
          draft-graph-2 (create-managed-graph-with-draft! live-graph-uri)]

      (delete-draft-graph! *test-backend* draft-graph-2)

      (is (= true (mgmt/draft-exists? *test-backend* draft-graph-1)))
      (is (= true (is-graph-managed? *test-backend* live-graph-uri))))))

;; This test attempts to capture the rationale behind the calculation of graph
;; restrictions.
;;
;; These tests attempt to recreate the various permutations of what will happen
;; when union-with-live=true/false and when there are drafts specified or not.
(deftest calculate-draft-raw-graphs-test
  (testing "union-with-live=true"
    (testing "with no drafts specified"
      (is (= #{:l1 :l2}
             (calculate-draft-raw-graphs #{:l1 :l2} #{} #{}))
          "Restriction should be the set of public live graphs"))

    (testing "with drafts specified"
      (is (= #{:l1 :d2 :d3 :d4}
             (calculate-draft-raw-graphs #{:l1 :l2} #{:l3 :l4 :l2} #{:d2 :d3 :d4}))
          "Restriction should be the set of live graphs plus drafts from the
            draftset.  Live graphs for the specified drafts should not be
            included as we want to use their draft graphs.")))

  (testing "union-with-live=false"
    (testing "with no drafts specified"
      (is (= #{}
             (calculate-draft-raw-graphs #{} #{} #{}))
          "Restriction should be the set of public live graphs"))
    (testing "with drafts specified"
      (is (= #{:d1 :d2}
             (calculate-draft-raw-graphs #{} #{:l1 :l2} #{:d1 :d2}))
          "Restriction should be the set of drafts (as live graph queries will be rewritten to their draft graphs)"))))

(defn test-quads [g]
  (map #(assoc % :c g)
       (test/test-triples (URI. "http://test-subject"))))

(deftest copy-graph-test
  (let [repo (repo/sail-repo)]
    (sparql/add repo (test-quads (URI. "http://test-graph/1")))

    (copy-graph repo (URI. "http://test-graph/1") (URI. "http://test-graph/2"))

    (let [source-graph (set (sparql/eager-query repo "SELECT * WHERE { GRAPH <http://test-graph/1> { ?s ?p ?o }}"))
          dest-graph   (set (sparql/eager-query repo "SELECT * WHERE { GRAPH <http://test-graph/2> { ?s ?p ?o }}"))]

      (is (not (empty? dest-graph))
          "Should not be empty (and have the data we loaded)")
      (is (= source-graph
             dest-graph)
          "Should be a copy of the source graph"))))

(defn- rewrite-in-draft
  "Creates a draft graph for the given live graph, inserts the given triples into the new draft graph and then
   rewrites the graph contents. Returns a map containing the draft graph UI and the re-written contents of
   the draft graph."
  [live-graph-uri live-triples]
  (let [repo (repo/sparql-repo "http://localhost:5820/drafter-test-db/query" "http://localhost:5820/drafter-test-db/update")
        graph-manager (graphs/create-manager repo)
        draftset (create-draftset! repo test-editor)
        draft-graph (graphs/create-user-graph-draft graph-manager draftset live-graph-uri)]
    (sparql/add repo draft-graph live-triples)
    (with-open [conn (repo/->connection repo)]
      (rewrite-draftset! conn {:draftset-uri (ds/->draftset-uri draftset)
                               :deleted :ignore}))
    (let [q (format "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <%s> { ?s ?p ?o } }" draft-graph)
          draft-triples (with-open [conn (repo/->connection repo)]
                          (set (repo/query conn q)))]
      {:draft-graph-uri draft-graph
       :draft-triples (set draft-triples)})))

(t/deftest rewrite-draftset!-graph-matches-s-test
  (let [live-graph (URI. "http://example.com/graph")
        triples #{(pr/->Triple live-graph (URI. "http://p") "o")}
        {:keys [draft-graph-uri draft-triples]} (rewrite-in-draft live-graph triples)]
    (t/is (= #{(pr/->Triple draft-graph-uri (URI. "http://p") "o")} draft-triples))))

(t/deftest rewrite-draftset!-graph-matches-p-test
  (let [live-graph (URI. "http://example.com/graph")
        triples #{(pr/->Triple (URI. "http://s") live-graph "o")}
        {:keys [draft-graph-uri draft-triples]} (rewrite-in-draft live-graph triples)]
    (t/is (= #{(pr/->Triple (URI. "http://s") draft-graph-uri "o")} draft-triples))))

(t/deftest rewrite-draftset!-graph-matches-o-test
  (let [live-graph (URI. "http://example.com/graph")
        s (URI. "http://s")
        p (URI. "http://p")
        triples #{(pr/->Triple s p live-graph)}
        {:keys [draft-graph-uri draft-triples]} (rewrite-in-draft live-graph triples)]
    (t/is (= #{(pr/->Triple s p draft-graph-uri)} draft-triples))))

(t/deftest rewrite-draftset!-graph-matches-sp-test
  (let [live-graph (URI. "http://example.com/graph")
        triples #{(pr/->Triple live-graph live-graph "o")}
        {:keys [draft-graph-uri draft-triples]} (rewrite-in-draft live-graph triples)]
    (t/is (= #{(pr/->Triple draft-graph-uri draft-graph-uri "o")} draft-triples))))

(t/deftest rewrite-draftset!-graph-matches-so-test
  (let [live-graph (URI. "http://example.com/graph")
        triples #{(pr/->Triple live-graph (URI. "http://p") live-graph)}
        {:keys [draft-graph-uri draft-triples]} (rewrite-in-draft live-graph triples)]
    (t/is (= #{(pr/->Triple draft-graph-uri (URI. "http://p") draft-graph-uri)} draft-triples))))

(t/deftest rewrite-draftset!-graph-matches-po-test
  (let [live-graph (URI. "http://example.com/graph")
        triples #{(pr/->Triple (URI. "http://s") live-graph live-graph)}
        {:keys [draft-graph-uri draft-triples]} (rewrite-in-draft live-graph triples)]
    (t/is (= #{(pr/->Triple (URI. "http://s") draft-graph-uri draft-graph-uri)} draft-triples))))

(t/deftest rewrite-draftset!-graph-matches-spo-test
  (let [live-graph (URI. "http://example.com/graph")
        triples #{(pr/->Triple live-graph live-graph live-graph)}
        {:keys [draft-graph-uri draft-triples]} (rewrite-in-draft live-graph triples)]
    (t/is (= #{(pr/->Triple draft-graph-uri draft-graph-uri draft-graph-uri)} draft-triples))))

(defn- raw-graph-triples [repo graphs]
  (into {} (map (fn [graph]
                  (let [q (format "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <%s> { ?s ?p ?o } }" graph)
                        bindings (with-open [conn (repo/->connection repo)]
                                   (vec (repo/query conn q)))]
                    [graph (set (map (fn [bs] (pr/map->Triple bs)) bindings))]))
                graphs)))

(t/deftest rewrite-draftset!-multiple-graph-references
  (let [live-graphs (map #(URI. (str "http://live-" %)) (range 1 5))
        [lg1 lg2 lg3 lg4] live-graphs
        quads {lg1 #{(pr/->Triple (URI. "http://s1") (URI. "http://p1") "o1")}
               lg2 #{(pr/->Triple lg1 (URI. "http://p2") "o2")
                     (pr/->Triple (URI. "http://s3") lg2 "o3")
                     (pr/->Triple (URI. "http://s4") (URI. "http://p4") lg3)}
               lg3 #{(pr/->Triple lg1 lg2 "o5")
                     (pr/->Triple (URI. "http://s6") lg2 lg3)
                     (pr/->Triple lg1 (URI. "http://p7") lg3)}
               lg4 #{(pr/->Triple lg4 lg4 lg4)
                     (pr/->Triple lg4 lg1 lg3)
                     (pr/->Triple lg1 lg2 lg3)
                     (pr/->Triple lg3 lg1 lg2)}}
        repo (repo/sparql-repo "http://localhost:5820/drafter-test-db/query" "http://localhost:5820/drafter-test-db/update")
        graph-manager (graphs/create-manager repo)
        draftset (create-draftset! repo test-editor)
        live->draft (into {} (map (fn [[lg triples]]
                                    (let [dg (graphs/create-user-graph-draft graph-manager draftset lg)]
                                      (sparql/add repo dg triples)
                                      [lg dg]))
                                  quads))]
    ;; re-write draftset
    (with-open [conn (repo/->connection repo)]
      (rewrite-draftset! conn {:draftset-uri (ds/->draftset-uri draftset)
                               :deleted :ignore}))

    (let [draftset-quads (raw-graph-triples repo (vals live->draft))
          rewrite-value (fn [v] (get live->draft v v))
          rewrite-triple (fn [t] (reduce (fn [t loc] (update t loc rewrite-value)) t [:s :p :o]))
          expected (into {} (map (fn [[lg ts]] [(get live->draft lg) (into #{} (map rewrite-triple ts))]) quads))]

      (t/is (= expected draftset-quads)))))

(defn- rewrite-draft-triples [draft-graph-uri draft-triples]
  (letfn [(rewrite-value [v] (if (= ::dg v) draft-graph-uri v))
          (rewrite-triple [t]
            (-> t
                (update :s rewrite-value)
                (update :p rewrite-value)
                (update :o rewrite-value)))]
    (into #{} (map rewrite-triple draft-triples))))

(defn- unrewrite-in-draft [live-graph-uri draft-triples]
  (let [repo (repo/sparql-repo "http://localhost:5820/drafter-test-db/query" "http://localhost:5820/drafter-test-db/update")
        graph-manager (graphs/create-manager repo)
        draftset (create-draftset! repo test-editor)
        draft-graph (graphs/create-user-graph-draft graph-manager draftset live-graph-uri)
        draft-triples (rewrite-draft-triples draft-graph draft-triples)]

    (with-open [conn (repo/->connection repo)]
      (sparql/add conn draft-graph draft-triples)
      (unrewrite-draftset! conn {:draftset-uri (ds/->draftset-uri draftset)
                                 :live-graph-uris [live-graph-uri]}))

    (let [q (format "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <%s> { ?s ?p ?o } }" draft-graph)]
      (with-open [conn (repo/->connection repo)]
        (set (repo/query conn q))))))

(t/deftest unrewrite-draftset-graph-matches-s-test
  (let [live-graph-uri (URI. "http://example.com")
        triples #{(pr/->Triple ::dg (URI. "http://p") "o")}
        unrewritten-triples (unrewrite-in-draft live-graph-uri triples)]
    (t/is (= #{(pr/->Triple live-graph-uri (URI. "http://p") "o")} unrewritten-triples))))

(t/deftest unrewrite-draftset-graph-matches-p-test
  (let [live-graph-uri (URI. "http://example.com")
        triples #{(pr/->Triple (URI. "http://s") ::dg "o")}
        unrewritten-triples (unrewrite-in-draft live-graph-uri triples)]
    (t/is (= #{(pr/->Triple (URI. "http://s") live-graph-uri "o")} unrewritten-triples))))

(t/deftest unrewrite-draftset-graph-matches-o-test
  (let [live-graph-uri (URI. "http://example.com")
        triples #{(pr/->Triple (URI. "http://s") (URI. "http://p") ::dg)}
        unrewritten-triples (unrewrite-in-draft live-graph-uri triples)]
    (t/is (= #{(pr/->Triple (URI. "http://s") (URI. "http://p") live-graph-uri)} unrewritten-triples))))

(t/deftest unrewrite-draftset-graph-matches-sp-test
  (let [live-graph-uri (URI. "http://example.com")
        triples #{(pr/->Triple ::dg ::dg "o")}
        unrewritten-triples (unrewrite-in-draft live-graph-uri triples)]
    (t/is (= #{(pr/->Triple live-graph-uri live-graph-uri "o")} unrewritten-triples))))

(t/deftest unrewrite-draftset-graph-matches-so-test
  (let [live-graph-uri (URI. "http://example.com")
        triples #{(pr/->Triple ::dg (URI. "http://p") ::dg)}
        unrewritten-triples (unrewrite-in-draft live-graph-uri triples)]
    (t/is (= #{(pr/->Triple live-graph-uri (URI. "http://p") live-graph-uri)} unrewritten-triples))))

(t/deftest unrewrite-draftset-graph-matches-po-test
  (let [live-graph-uri (URI. "http://example.com")
        triples #{(pr/->Triple (URI. "http://s") ::dg ::dg)}
        unrewritten-triples (unrewrite-in-draft live-graph-uri triples)]
    (t/is (= #{(pr/->Triple (URI. "http://s") live-graph-uri live-graph-uri)} unrewritten-triples))))

(t/deftest unrewrite-draftset!-graph-matches-spo-test
  (let [live-graph-uri (URI. "http://example.com")
        triples #{(pr/->Triple ::dg ::dg ::dg)}
        unrewritten-triples (unrewrite-in-draft live-graph-uri triples)]

    (t/is (= #{(pr/->Triple live-graph-uri live-graph-uri live-graph-uri)} unrewritten-triples))))

(defn- rewrite-triples [graph-mapping graph->triples]
  (letfn [(rewrite-value [v] (get graph-mapping v v))
          (rewrite-triple [triple]
            (reduce (fn [t loc] (update t loc rewrite-value)) triple [:s :p :o]))]
    (util/map-values (fn [triples] (into #{} (map rewrite-triple triples))) graph->triples)))

(t/deftest unrewrite-draftset!-multiple-graph-references
  (let [repo (repo/sparql-repo "http://localhost:5820/drafter-test-db/query" "http://localhost:5820/drafter-test-db/update")
        graph-manager (graphs/create-manager repo)
        live-graphs (map #(URI. (str "http://live-" %)) (range 1 5))
        [lg1 lg2 lg3 lg4] live-graphs
        draftset (create-draftset! repo test-editor)
        draft-graphs (mapv (fn [lg] (graphs/create-user-graph-draft graph-manager draftset lg)) live-graphs)
        [dg1 dg2 dg3 dg4] draft-graphs
        draft-quads {dg1 #{(pr/->Triple (URI. "http://s1") (URI. "http://p1") "o1")}
                     dg2 #{(pr/->Triple dg1 (URI. "http://p2") "o2")
                           (pr/->Triple (URI. "http://s3") dg2 "o3")
                           (pr/->Triple (URI. "http://s4") (URI. "http://p4") dg3)}
                     dg3 #{(pr/->Triple dg1 dg2 "o5")
                           (pr/->Triple (URI. "http://s6") dg2 dg3)
                           (pr/->Triple dg1 (URI. "http://p7") dg3)}
                     dg4 #{(pr/->Triple dg4 dg4 dg4)
                           (pr/->Triple dg4 dg1 dg3)
                           (pr/->Triple dg1 dg2 dg3)
                           (pr/->Triple dg3 dg1 dg2)}}]
    (with-open [conn (repo/->connection repo)]
      ;; add draft triples
      (doseq [[dg triples] draft-quads]
        (sparql/add conn dg triples))

      ;;unrewrite draft
      (unrewrite-draftset! conn {:draftset-uri (ds/->draftset-uri draftset)
                                 :live-graph-uris live-graphs}))

    (let [expected (rewrite-triples (zipmap draft-graphs live-graphs) draft-quads)
          actual (raw-graph-triples repo draft-graphs)]
      (t/is (= expected actual)))))

(use-fixtures :each (wrap-system-setup "test-system.edn" [:drafter.stasher/repo :drafter/write-scheduler]))
;(use-fixtures :each wrap-clean-test-db)
