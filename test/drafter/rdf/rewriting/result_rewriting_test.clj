(ns drafter.rdf.rewriting.result-rewriting-test
  (:require [clojure
             [set :as set]
             [test :refer :all]]
            [drafter
             [test-common :refer [*test-backend* test-triples wrap-clean-test-db wrap-system-setup]]
             [util :refer [map-values]]]
            [drafter.backend.protocols :refer [prepare-query]]
            [drafter.rdf.draft-management
             :refer
             [append-data-batch! create-draft-graph!]]
            [drafter.rdf.rewriting.query-rewriting :refer [rewrite-sparql-string]]
            [grafter.rdf4j.repository :as repo]
            [grafter.rdf
             [templater :refer [triplify]]]
            [schema.test :refer [validate-schemas]]
            [clojure.java.io :as io])
  (:import org.eclipse.rdf4j.model.impl.URIImpl
           [java.net URI]))

(use-fixtures :each validate-schemas)

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
    (map #(rewrite-result result-substitutions %) result)))

(defn evaluate-with-graph-rewriting
  "Rewrites the results in the query."
  [db query-str query-substitutions]
  (let [rewritten-query (rewrite-sparql-string query-substitutions query-str)
        prepared-query (prepare-query db rewritten-query)]
    (rewrite-graph-results query-substitutions prepared-query)))

(defn first-result [results key]
  (-> results first (get key)))

(defn first-result-keys [results ks]
  ((apply juxt ks) (first results)))

(deftest query-and-result-rewriting-test
  (let [draft-graph (create-draft-graph! *test-backend* (URI. "http://frogs.com/live-graph"))
        graph-map {(URI. "http://frogs.com/live-graph") draft-graph}]

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
    (let [draft-graph (create-draft-graph! *test-backend* (URI. "http://frogs.com/live-graph"))
          live-triple [(URI. "http://live-subject") (URI. "http://live-predicate") (URI. "http://live-object")]
          draft-triple [(URI. "http://draft-subject") (URI. "http://draft-predicate") (URI. "http://draft-object")]
          [draft-subject draft-predicate draft-object] draft-triple
          graph-map (zipmap live-triple draft-triple)]

      (append-data-batch! *test-backend* draft-graph (triplify [draft-subject [draft-predicate draft-object]]))

      (let [query "SELECT * WHERE { ?s ?p ?o }"
            results (evaluate-with-graph-rewriting *test-backend* query graph-map)]

        (is (some #{live-triple} (map (juxt :s :p :o) results)))))))

(use-fixtures :once (wrap-system-setup (io/resource "test-system.edn") [:drafter.backend.rdf4j/remote :drafter/write-scheduler]))
(use-fixtures :each wrap-clean-test-db)
