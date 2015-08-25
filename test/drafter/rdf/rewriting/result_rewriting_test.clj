(ns drafter.rdf.rewriting.result-rewriting-test
  (:require
   [drafter.test-common :refer [test-triples make-backend]]
   [drafter.rdf.draft-management :refer [create-managed-graph create-draft-graph!]]
   [drafter.backend.protocols :refer [append-data-batch!]]
   [grafter.rdf.templater :refer [graph triplify]]
   [grafter.rdf :refer [statements]]
   [grafter.rdf.repository :refer [repo prepare-update]]
   [clojure.test :refer :all]
   [drafter.rdf.rewriting.result-rewriting :refer [evaluate-with-graph-rewriting]])
  (:import
           [org.openrdf.model.impl URIImpl]
           [org.openrdf.query QueryLanguage]
           [org.openrdf.query.parser QueryParserUtil]
           [org.openrdf.query.algebra.evaluation.function Function]
           [org.openrdf.query.algebra Var StatementPattern Extension ExtensionElem FunctionCall]
           [org.openrdf.query.algebra.helpers QueryModelTreePrinter VarNameCollector StatementPatternCollector QueryModelVisitorBase]))

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
  (let [[db backend] (make-backend)
        draft-graph (create-draft-graph! db "http://frogs.com/live-graph")
        graph-map {(URIImpl. "http://frogs.com/live-graph") (URIImpl. draft-graph)}]

    (append-data-batch! backend draft-graph (test-triples "http://kermit.org/the-frog"))

    (testing "rewrites subject URIs"
      (let [query "SELECT * WHERE { GRAPH <http://frogs.com/live-graph> { <http://kermit.org/the-frog> ?p ?o } }"
            mapped-subject "http://mapped-subject"
            graph-map (assoc graph-map (URIImpl. "http://kermit.org/the-frog") (URIImpl. mapped-subject))
            po ["http://predicate" "http://object"]
            mapped-triples (triplify [mapped-subject po])]

        (append-data-batch! backend draft-graph mapped-triples)

        (is (= po
               (-> db
                   (evaluate-with-graph-rewriting query graph-map)
                   (first-result-keys ["p" "o"]))))))

    (testing "rewrites predicate URIs"
      (let [query "SELECT * WHERE { GRAPH <http://frogs.com/live-graph> { ?s <http://source-predicate> ?o } }"
            mapped-predicate "http://mapped-predicate"
            graph-map (assoc graph-map (URIImpl. "http://source-predicate") (URIImpl. mapped-predicate))
            mapped-triples (triplify ["http://kermit.org/the-frog" [mapped-predicate "http://object"]])]

        (append-data-batch! backend draft-graph mapped-triples)

        (is (= ["http://kermit.org/the-frog" "http://object"]
               (-> db
                   (evaluate-with-graph-rewriting query graph-map)
                   (first-result-keys ["s" "o"]))))))

    (testing "rewrites object URIs"
      (let [query "SELECT * WHERE { GRAPH <http://frogs.com/live-graph> { ?s ?p <http://source-object> } }"
            mapped-object "http://mapped-object"
            graph-map (assoc graph-map (URIImpl. "http://source-object") (URIImpl. mapped-object))
            mapped-triples (triplify ["http://kermit.org/the-frog" ["http://predicate" mapped-object]])]

        (append-data-batch! backend draft-graph mapped-triples)

        (is (= ["http://kermit.org/the-frog" "http://predicate"]
               (-> db
                   (evaluate-with-graph-rewriting query graph-map)
                   (first-result-keys ["s" "p"]))))))

    (testing "rewrites all result URIs"
      (let [query "SELECT * WHERE { ?s ?p ?o }"
            [db backend] (make-backend)
            live-triple ["http://live-subject" "http://live-predicate" "http://live-object"]
            draft-triple ["http://draft-subject" "http://draft-predicate" "http://draft-object"]
            [live-subject live-predicate live-object] live-triple
            [draft-subject draft-predicate draft-object] draft-triple
            graph-map (apply hash-map (map #(URIImpl. %) (interleave live-triple draft-triple)))]

        (append-data-batch! backend draft-graph (triplify [draft-subject [draft-predicate draft-object]]))

        (is (= live-triple
               (-> db
                   (evaluate-with-graph-rewriting query graph-map)
                   (first-result-keys ["s" "p" "o"]))))))

    (testing "rewrites query to query draft graph"
      ;; NOTE this query rewrites the URI constant <http://frogs.com/live-graph>
      (is (= "http://kermit.org/the-frog"
             (-> db
                 (evaluate-with-graph-rewriting graph-constant-query graph-map)
                 (first-result "s"))))

    (testing "rewrites SPARQL URI/IRI functions to query with substitution"

      (comment
        (register-function! function-registry (pmdfunctions "replace-live-graph-uri") graph-map)
        ;; NOTE this query rewrites dynamically.  It does not rewrite
        ;; the constant string "http://frogs.com/live-graph" but instead
        ;; rewrites it at runtime via the function composition.
        (is (= "http://kermit.org/the-frog"
               (-> db
                   (evaluate-with-graph-rewriting uri-query graph-map)
                   (first-result "s"))))

        (testing "rewrites results when graph is in selection"
          ;; NOTE this query rewrites dynamically.  It does not rewrite
          ;; the constant string "http://frogs.com/live-graph" but instead
          ;; rewrites it at runtime via the function composition.
          (is (= "http://frogs.com/live-graph"
                 (-> db
                     (evaluate-with-graph-rewriting uri-query graph-map)
                     (first-result "g"))))))

      (testing "rewrites source graphs in VALUES clause"
        (is (= "http://frogs.com/live-graph"
               (-> db
                   (evaluate-with-graph-rewriting graph-values-query graph-map)
                   (first-result "g")))))

      (testing "rewrites source graph literals in FILTER clause"
        (is (= "http://frogs.com/live-graph"
               (-> db
                   (evaluate-with-graph-rewriting graph-filter-query graph-map)
                   (first-result "g")))))))))
