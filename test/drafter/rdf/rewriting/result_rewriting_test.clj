(ns drafter.rdf.rewriting.result-rewriting-test
  (:require
   [drafter.test-common :refer [test-triples *test-backend* wrap-db-setup wrap-clean-test-db]]
   [clojure.set :as set]
   [drafter.util :refer [map-values]]
   [drafter.rdf.draft-management :refer [create-managed-graph create-draft-graph! append-data-batch!]]
   [drafter.backend.protocols :refer [prepare-query]]
   [grafter.rdf.templater :refer [graph triplify]]
   [grafter.rdf :refer [statements]]
   [grafter.rdf.repository :refer [repo prepare-update] :as repo]
   [drafter.rdf.rewriting.query-rewriting :refer [rewrite-sparql-string]]
   [clojure.test :refer :all]
   [schema.test :refer [validate-schemas]])

  (:import
           [org.openrdf.model.impl URIImpl]
           [org.openrdf.query QueryLanguage]
           [org.openrdf.query.parser QueryParserUtil]
           [org.openrdf.query.algebra.evaluation.function Function]
           [org.openrdf.query.algebra Var StatementPattern Extension ExtensionElem FunctionCall]
           [org.openrdf.query.algebra.helpers QueryModelTreePrinter VarNameCollector StatementPatternCollector QueryModelVisitorBase]))

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
  (-> results first (get key) str))

(defn juxt-keys [ks]
  (let [fns (map (fn [k] (fn [m] (get m k))) ks)]
    (apply juxt fns)))

(defn result-keys [m ks]
  (mapv str ((juxt-keys ks) m)))

(defn first-result-keys [results ks]
  (result-keys (first results) ks))

(deftest query-and-result-rewriting-test
  (let [draft-graph (create-draft-graph! *test-backend* "http://frogs.com/live-graph")
        graph-map {(URIImpl. "http://frogs.com/live-graph") (URIImpl. draft-graph)}]

    (append-data-batch! *test-backend* draft-graph (test-triples "http://kermit.org/the-frog"))

    (testing "rewrites subject URIs"
      (let [query "SELECT * WHERE { GRAPH <http://frogs.com/live-graph> { <http://kermit.org/the-frog> ?p ?o } }"
            mapped-subject "http://mapped-subject"
            graph-map (assoc graph-map (URIImpl. "http://kermit.org/the-frog") (URIImpl. mapped-subject))
            po ["http://predicate" "http://object"]
            mapped-triples (triplify [mapped-subject po])]

        (append-data-batch! *test-backend* draft-graph mapped-triples)

        (is (= po
               (-> *test-backend*
                   (evaluate-with-graph-rewriting query graph-map)
                   (first-result-keys ["p" "o"]))))))

    (testing "rewrites predicate URIs"
      (let [query "SELECT * WHERE { GRAPH <http://frogs.com/live-graph> { ?s <http://source-predicate> ?o } }"
            mapped-predicate "http://mapped-predicate"
            graph-map (assoc graph-map (URIImpl. "http://source-predicate") (URIImpl. mapped-predicate))
            mapped-triples (triplify ["http://kermit.org/the-frog" [mapped-predicate "http://object"]])]

        (append-data-batch! *test-backend* draft-graph mapped-triples)

        (is (= ["http://kermit.org/the-frog" "http://object"]
               (-> *test-backend*
                   (evaluate-with-graph-rewriting query graph-map)
                   (first-result-keys ["s" "o"]))))))

    (testing "rewrites object URIs"
      (let [query "SELECT * WHERE { GRAPH <http://frogs.com/live-graph> { ?s ?p <http://source-object> } }"
            mapped-object "http://mapped-object"
            graph-map (assoc graph-map (URIImpl. "http://source-object") (URIImpl. mapped-object))
            mapped-triples (triplify ["http://kermit.org/the-frog" ["http://predicate" mapped-object]])]

        (append-data-batch! *test-backend* draft-graph mapped-triples)

        (is (= ["http://kermit.org/the-frog" "http://predicate"]
               (-> *test-backend*
                   (evaluate-with-graph-rewriting query graph-map)
                   (first-result-keys ["s" "p"]))))))

    (testing "rewrites query to query draft graph"
      ;; NOTE this query rewrites the URI constant <http://frogs.com/live-graph>
      (is (= "http://kermit.org/the-frog"
             (-> *test-backend*
                 (evaluate-with-graph-rewriting graph-constant-query graph-map)
                 (first-result "s"))))

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
        (is (= "http://frogs.com/live-graph"
               (-> *test-backend*
                   (evaluate-with-graph-rewriting graph-values-query graph-map)
                   (first-result "g")))))

      (testing "rewrites source graph literals in FILTER clause"
        (is (= "http://frogs.com/live-graph"
               (-> *test-backend*
                   (evaluate-with-graph-rewriting graph-filter-query graph-map)
                   (first-result "g")))))))))

(deftest more-query-and-result-rewriting-test
  (testing "rewrites all result URIs"
    (let [draft-graph (create-draft-graph! *test-backend* "http://frogs.com/live-graph")
          live-triple ["http://live-subject" "http://live-predicate" "http://live-object"]
          draft-triple ["http://draft-subject" "http://draft-predicate" "http://draft-object"]
          [live-subject live-predicate live-object] live-triple
          [draft-subject draft-predicate draft-object] draft-triple
          graph-map (zipmap (map #(URIImpl. %) live-triple)      ;; live-uri -> draft-uri
                            (map #(URIImpl. %) draft-triple))]

      (append-data-batch! *test-backend* draft-graph (triplify [draft-subject [draft-predicate draft-object]]))

      (let [query "SELECT * WHERE { ?s ?p ?o }"
            results (evaluate-with-graph-rewriting *test-backend* query graph-map)]

        (is (some #{live-triple}
                  (map #(result-keys % ["s" "p" "o"]) results)))))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
