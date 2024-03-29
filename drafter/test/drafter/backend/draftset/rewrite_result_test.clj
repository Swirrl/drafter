(ns drafter.backend.draftset.rewrite-result-test
  (:require [drafter.backend.draftset.rewrite-result :refer :all]
            [clojure.test :as t :refer :all]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [drafter.backend.draftset.draft-management :refer [append-data-batch!]]
            [drafter.backend.draftset.rewrite-query :refer [rewrite-sparql-string]]
            [drafter.test-common
             :refer
             [*test-backend* test-triples wrap-system-setup]
             :as tc]
            [drafter.util :refer [map-values]]
            [grafter-2.rdf4j.templater :refer [triplify]]
            [grafter-2.rdf4j.repository :as repo]
            [schema.test :refer [validate-schemas]]
            [grafter-2.rdf4j.io :as gio]
            [grafter-2.rdf.protocols :as pr]
            [drafter.rdf.dataset :as dataset]
            [drafter.backend.draftset.graphs :as graphs]
            [drafter.user-test :refer [test-editor]]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.backend.draftset.query-impl :as query-impl]
            [drafter.fixture-data :as fd])
  (:import java.net.URI))

(use-fixtures :each
  validate-schemas
  tc/with-spec-instrumentation)

(def uri-query
  "SELECT * WHERE {
     BIND(URI(\"http://frogs.com/live-graph\") AS ?g)
     GRAPH ?g {
       ?s ?p ?o
     }
   } LIMIT 10")

(def graph-constant-query
  "SELECT * WHERE {
     GRAPH <http://frogs.com/live-graph> {
       ?s ?p ?o
     }
   } LIMIT 10")

(def graph-values-query
  "SELECT * WHERE {
     GRAPH ?g {
       ?s ?p ?o
     }
     VALUES ?g { <http://frogs.com/live-graph> }
   }")

(def graph-filter-query
  "SELECT * WHERE {
    GRAPH ?g {
      ?s ?p ?o
    }
    FILTER( ?g = <http://frogs.com/live-graph> )
    }")

;apply-map-or-default :: Map[a, a] -> (a -> a)
(defn- apply-map-or-default
  "Returns a function with a single argument. When applied, it sees if
  the value is a key in the source map - if it exists, the
  corresponding value is returned, otherwise the argument is
  returned."
  [m]
  (fn [r] (get m r r)))

;rewrite-result :: Map[Uri, Uri] -> Map[a, Uri] -> Map[a, Uri]
(defn- rewrite-result
  "Rewrites all the values in a result based on the given draft->live
  graph mapping."
  [graph-map r]
  (map-values (apply-map-or-default graph-map) r))

(defn- rewrite-graph-results [query-substitutions prepared-query]
  "Executes the given prepared query and rewrites the results
  according to the given subtstitutions. WARNING: This assumes the
  prepared query is a Sesame query type! Consider adding a method to
  execute prepared queries on the SPARQLExecutor protocol!"
  (let [result-substitutions (set/map-invert query-substitutions)
        result (repo/evaluate prepared-query)]
    (doall
     (map #(rewrite-result result-substitutions %) result))))

(defn evaluate-with-graph-rewriting
  "Rewrites the results in the query."
  [db query-str query-substitutions]
  (with-open [conn (repo/->connection db)]
    (let [rewritten-query (rewrite-sparql-string query-substitutions query-str)
          prepared-query (repo/prepare-query conn rewritten-query)]
      (rewrite-graph-results query-substitutions prepared-query))))

(defn first-result [results key]
  (-> results first (get key)))

(defn first-result-keys [results ks]
  ((apply juxt ks) (first results)))

(deftest query-and-result-rewriting-test
  (let [draftset-id (dsops/create-draftset! *test-backend* test-editor)
        graph-manager (graphs/create-manager *test-backend*)
        live-graph (URI. "http://frogs.com/live-graph")
        draft-graph (graphs/create-user-graph-draft graph-manager draftset-id live-graph)
        graph-map {live-graph draft-graph}]

    (append-data-batch! *test-backend* draft-graph (test-triples (URI. "http://kermit.org/the-frog")))

    (testing "rewrites subject URIs"
      (let [query "SELECT * WHERE { GRAPH <http://frogs.com/live-graph> { <http://kermit.org/the-frog> ?p ?o } }"
            mapped-subject (URI. "http://mapped-subject")
            graph-map (assoc graph-map (URI. "http://kermit.org/the-frog") mapped-subject)
            po [(URI. "http://predicate") (URI. "http://object")]
            mapped-triples (triplify [mapped-subject po])]

        (append-data-batch! *test-backend* draft-graph mapped-triples)

        (let [results (evaluate-with-graph-rewriting *test-backend* query graph-map)
              result-po (first-result-keys results [:p :o])]
          (is (= result-po po)))))

    (testing "rewrites predicate URIs"
      (let [query "SELECT * WHERE { GRAPH <http://frogs.com/live-graph> { ?s <http://source-predicate> ?o } }"
            mapped-predicate (URI. "http://mapped-predicate")
            graph-map (assoc graph-map (URI. "http://source-predicate") mapped-predicate)
            mapped-triples (triplify [(URI. "http://kermit.org/the-frog") [mapped-predicate (URI. "http://object")]])]

        (append-data-batch! *test-backend* draft-graph mapped-triples)

        (let [results (evaluate-with-graph-rewriting *test-backend* query graph-map)
              result-so (first-result-keys results [:s :o])]
          (is (= [(URI. "http://kermit.org/the-frog") (URI. "http://object")] result-so)))))

    (testing "rewrites object URIs"
      (let [query "SELECT * WHERE { GRAPH <http://frogs.com/live-graph> { ?s ?p <http://source-object> } }"
            mapped-object (URI. "http://mapped-object")
            graph-map (assoc graph-map (URI. "http://source-object") mapped-object)
            mapped-triples (triplify [(URI. "http://kermit.org/the-frog") [(URI. "http://predicate") mapped-object]])]

        (append-data-batch! *test-backend* draft-graph mapped-triples)

        (is (= [(URI. "http://kermit.org/the-frog") (URI. "http://predicate")]
               (-> *test-backend*
                   (evaluate-with-graph-rewriting query graph-map)
                   (first)
                   ((juxt :s :p)))))))

    (testing "rewrites query to query draft graph"
      ;; NOTE this query rewrites the URI constant <http://frogs.com/live-graph>
      (is (= (URI. "http://kermit.org/the-frog")
             (-> *test-backend*
                 (evaluate-with-graph-rewriting graph-constant-query graph-map)
                 (first-result :s))))

    (testing "rewrites SPARQL URI/IRI functions to query with substitution"

      (comment
        (register-function! function-registry (pmdfunctions "replace-live-graph-uri") graph-map)
        ;; NOTE this query rewrites dynamically.  It does not rewrite
        ;; the constant string "http://frogs.com/live-graph" but instead
        ;; rewrites it at runtime via the function composition.
        (is (= "http://kermit.org/the-frog"
               (-> *test-backend*
                   (evaluate-with-graph-rewriting uri-query graph-map)
                   (first-result "s"))))

        (testing "rewrites results when graph is in selection"
          ;; NOTE this query rewrites dynamically.  It does not rewrite
          ;; the constant string "http://frogs.com/live-graph" but instead
          ;; rewrites it at runtime via the function composition.
          (is (= "http://frogs.com/live-graph"
                 (-> *test-backend*
                     (evaluate-with-graph-rewriting uri-query graph-map)
                     (first-result "g"))))))

      (testing "rewrites source graphs in VALUES clause"
        (is (= (URI. "http://frogs.com/live-graph")
               (-> *test-backend*
                   (evaluate-with-graph-rewriting graph-values-query graph-map)
                   (first-result :g)))))

      (testing "rewrites source graph literals in FILTER clause"
        (is (= (URI. "http://frogs.com/live-graph")
               (-> *test-backend*
                   (evaluate-with-graph-rewriting graph-filter-query graph-map)
                   (first-result :g)))))))))

(deftest more-query-and-result-rewriting-test
  (testing "rewrites all result URIs"
    (let [graph-manager (graphs/create-manager *test-backend*)
          draftset-id (dsops/create-draftset! *test-backend* test-editor)
          live-graph-uri (URI. "http://frogs.com/live-graph")
          draft-graph (graphs/create-user-graph-draft graph-manager draftset-id live-graph-uri)
          live-triple [(URI. "http://live-subject") (URI. "http://live-predicate") (URI. "http://live-object")]
          draft-triple [(URI. "http://draft-subject") (URI. "http://draft-predicate") (URI. "http://draft-object")]
          [draft-subject draft-predicate draft-object] draft-triple
          graph-map (zipmap live-triple draft-triple)]

      (append-data-batch! *test-backend* draft-graph (triplify [draft-subject [draft-predicate draft-object]]))

      (let [query "SELECT * WHERE { ?s ?p ?o }"
            results (evaluate-with-graph-rewriting *test-backend* query graph-map)]

        (is (some #{live-triple} (map (juxt :s :p :o) results)))))))

(deftest rewritten-query-operations-test
  (tc/with-system
    [:drafter.stasher/repo]
    [system "drafter/feature/empty-db-system.edn"]
    (let [query-endpoint (:drafter.common.config/sparql-query-endpoint system)
          update-endpoint (:drafter.common.config/sparql-update-endpoint system)
          repo (repo/sparql-repo query-endpoint update-endpoint)
          select-spog "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }"
          construct-spo "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH ?g { ?s ?p ?o } }"
          live->draft {(gio/->rdf4j-uri "http://test.com/graph-1") (gio/->rdf4j-uri "http://test.com/draft-1")}
          test-data (io/resource "drafter/backend/draftset/rewrite_result_test/query-operations.trig")]

      (fd/load-fixture! {:repo repo :format :trig :fixtures [test-data]})

      (t/testing "boolean query"
        (t/testing "Operation methods"
          (with-open [conn (repo/->connection repo)]
            (let [query (rewriting-query (repo/prepare-query conn "ASK WHERE { GRAPH ?g { ?s ?p ?o . } }") live->draft)]
              (query-impl/test-operation-methods query #{"g" "s" "p" "o"}))))

        (t/testing "bindings"
          (with-open [conn (repo/->connection repo)]
            (let [query (rewriting-query (repo/prepare-query conn "ASK WHERE { GRAPH ?g { <http://test.com/subject-1> <http://test.com/p2> true . } }") live->draft)]
              (.setBinding query "g" (gio/->rdf4j-uri "http://test.com/graph-1"))
              (let [result (repo/evaluate query)]
                (is (= true result) "Unexpected query results")))))

        (t/testing "dataset"
          (with-open [conn (repo/->connection repo)]
            (let [query (rewriting-query (repo/prepare-query conn "ASK WHERE { <http://test.com/subject-1> <http://test.com/p2> true . }") live->draft)
                  restriction (dataset/create :named-graphs ["http://test.com/graph-1"
                                                             "http://test.com/graph-2"]
                                              :default-graphs ["http://test.com/graph-1"])]
              (.setDataset query (dataset/->rdf4j-dataset restriction))

              (let [result (repo/evaluate query)]
                (is (= true result) "Unexpected results of restricted query"))))))

      (t/testing "tuple query"
        (t/testing "Operation methods"
          (with-open [conn (repo/->connection repo)]
            (let [query (rewriting-query (repo/prepare-query conn select-spog) live->draft)]
              (query-impl/test-operation-methods query #{"s" "p" "o" "g"}))))

        (t/testing "bindings"
          (with-open [conn (repo/->connection repo)]
            (let [query (rewriting-query (repo/prepare-query conn select-spog) live->draft)]
              (.setBinding query "g" (gio/->rdf4j-uri "http://test.com/graph-1"))

              (let [results (repo/evaluate query)
                    ;; should return data from draft-1 graph according to the mapping from <graph-1> bound to ?g
                    ;; the <draft-1> object in the graph should be re-written to the corresponding live graph in the
                    ;; graph mapping (<graph-1>)
                    expected #{{:s (URI. "http://test.com/subject-1") :p (URI. "http://test.com/p1") :o "draft"}
                               {:s (URI. "http://test.com/subject-1") :p (URI. "http://test.com/g") :o (URI. "http://test.com/graph-1")}
                               {:s (URI. "http://test.com/subject-1") :p (URI. "http://test.com/p2") :o true}}]
                (is (= expected (set results)) "Unexpected query results")))))

        (t/testing "dataset"
          (with-open [conn (repo/->connection repo)]
            (let [query (rewriting-query (repo/prepare-query conn select-spog) live->draft)
                  restriction (dataset/create :named-graphs ["http://test.com/graph-1"
                                                             "http://test.com/graph-2"]
                                              :default-graphs ["http://test.com/graph-1"])]
              (.setDataset query (dataset/->rdf4j-dataset restriction))

              (let [results (repo/evaluate query)
                    ;; only results from restricted graphs should be returned
                    ;; restriction on <graph-1> should be converted to one on <draft-1> and the <graph-1> object in the
                    ;; graph should be re-written in the results
                    expected #{{:s (URI. "http://test.com/subject-1") :p (URI. "http://test.com/p1") :o "draft" :g (URI. "http://test.com/graph-1")}
                               {:s (URI. "http://test.com/subject-1") :p (URI. "http://test.com/g") :o (URI. "http://test.com/graph-1") :g (URI. "http://test.com/graph-1")}
                               {:s (URI. "http://test.com/subject-1") :p (URI. "http://test.com/p2") :o true :g (URI. "http://test.com/graph-1")}
                               {:s (URI. "http://test.com/subject-2") :p (URI. "http://test.com/p1") :o "second" :g (URI. "http://test.com/graph-2")}}]
                (is (= expected (set results)) "Unexpected results of restricted query"))))))

      (t/testing "graph query"
        (t/testing "Operation methods"
          (with-open [conn (repo/->connection repo)]
            (let [query (rewriting-query (repo/prepare-query conn construct-spo) live->draft)]
              (query-impl/test-operation-methods query #{"s" "p" "o" "g"}))))

        (t/testing "bindings"
          (with-open [conn (repo/->connection repo)]
            (let [query (rewriting-query (repo/prepare-query conn construct-spo) live->draft)]
              (.setBinding query "g" (gio/->rdf4j-uri "http://test.com/graph-1"))

              (let [results (repo/evaluate query)
                    ;; should return data from <draft-1> graph according to the mapping from <graph-1> bound to ?g
                    ;; the <draft-1> object in the graph should be re-written to the corresponding live graph in the
                    ;; graph mapping (<graph-1>)
                    expected #{(pr/->Triple (URI. "http://test.com/subject-1") (URI. "http://test.com/p1") "draft")
                               (pr/->Triple (URI. "http://test.com/subject-1") (URI. "http://test.com/g") (URI. "http://test.com/graph-1"))
                               (pr/->Triple (URI. "http://test.com/subject-1") (URI. "http://test.com/p2") true)}]
                (is (= expected (set results)) "Unexpected query results")))))

        (t/testing "dataset"
          (with-open [conn (repo/->connection repo)]
            (let [restriction (dataset/create :named-graphs ["http://test.com/graph-1"
                                                             "http://test.com/graph-2"]
                                              :default-graphs ["http://test.com/graph-1"])
                  query (rewriting-query (repo/prepare-query conn construct-spo) live->draft)]
              (.setDataset query (dataset/->rdf4j-dataset restriction))

              (let [results (repo/evaluate query)
                    ;; only results from restricted graphs should be returned
                    ;; restriction on <graph-1> should be converted to one on <draft-1> and the <graph-1> object in the
                    ;; graph should be re-written in the results
                    expected #{(pr/->Triple (URI. "http://test.com/subject-1") (URI. "http://test.com/p1") "draft")
                               (pr/->Triple (URI. "http://test.com/subject-1") (URI. "http://test.com/g") (URI. "http://test.com/graph-1"))
                               (pr/->Triple (URI. "http://test.com/subject-1") (URI. "http://test.com/p2") true)
                               (pr/->Triple (URI. "http://test.com/subject-2") (URI. "http://test.com/p1") "second")}]
                (is (= expected (set results)) "Unexpected results of restricted query")))))))))

(use-fixtures :each (wrap-system-setup "test-system.edn" [:drafter.stasher/repo :drafter/write-scheduler]))
;(use-fixtures :each wrap-clean-test-db)
