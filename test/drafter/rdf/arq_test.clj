(ns drafter.rdf.arq-test
  (:require [drafter.rdf.arq :refer :all]
            [clojure.test :refer :all]
            [clojure.string :refer [trim]])
  (:import [org.apache.jena.query Query]
           [org.apache.jena.sparql.sse Item ItemList]
           [org.apache.jena.sparql.algebra Op]
           [org.apache.jena.graph NodeFactory]))

(defn rewrite [rewriter q]
  (-> q
      sparql-string->arq-query
      ->sse-item
      sse-zipper
      rewriter
      str))

(def substitutions {"http://foo.com/" "http://foo.com/replaced"
                    "http://bar.com/" "http://bar.com/replaced"})

(defn as-ast-str [q]
  (-> q
      sparql-string->arq-query
      ->sse-item
      str))

(defn is-tree=
  "Compares an expected SPARQL query string with the tree"
  [expected test]
  (is (= (as-ast-str expected) (str (->sse-item test)))))

(def substitute-uris (partial named-graphs-rewriter substitutions))

(defn has-prefix-mapping? [q]
  (not (empty? (.getNsPrefixMap (.getPrefixMapping q)))))

(deftest apply-rewriter-test
  (testing "Restores query clause"

    (let [rewrite->query (partial apply-rewriter substitute-uris)]

      (are [method qstr]
        (is (method (rewrite->query qstr)))
        .isDescribeType            "DESCRIBE <http://foo.com/> <http://bar.com/> <http://unreplaced.com/>"
        .isDescribeType            "DESCRIBE ?s WHERE { ?s ?p ?o }"
        .isSelectType              "SELECT * WHERE { ?s ?p ?o }"
        .isDistinct                "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"
        .isReduced                 "SELECT REDUCED ?s WHERE { ?s ?p ?o }"
        .isOrdered                 "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s"
        .hasLimit                  "SELECT * WHERE { ?s ?p ?o } LIMIT 10"
        .hasOffset                  "SELECT * WHERE { ?s ?p ?o } OFFSET 10"
        .hasDatasetDescription     "SELECT * FROM <http://foo.com> WHERE { ?s ?p ?o }"
        .hasDatasetDescription     "SELECT * FROM NAMED <http://foo.com> WHERE { ?s ?p ?o }"
        .isAskType                 "ASK WHERE { ?s ?p ?o }"
        .isConstructType           "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
        .getConstructTemplate      "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"

        has-prefix-mapping?        "PREFIX foo: <http://foo.com> CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
        has-prefix-mapping?        "PREFIX foo: <http://foo.com> SELECT * WHERE { ?s ?p ?o }"
        has-prefix-mapping?        "PREFIX foo: <http://foo.com> ASK WHERE { ?s ?p ?o }"
        has-prefix-mapping?        "PREFIX foo: <http://foo.com> DESCRIBE <http://foo.com>"

        ;; TODO test .getPrefixMappings on all query types
        ))))

(deftest primitive-describe-rewriting
  (let [rewritten-describe (trim (str (apply-rewriter substitute-uris
                                                      "DESCRIBE <http://foo.com/> <http://bar.com/> <http://unreplaced.com/>")))]
    (is (= "DESCRIBE <http://foo.com/replaced> <http://bar.com/replaced> <http://unreplaced.com/>" rewritten-describe))))

(deftest named-graphs-rewriter-test
  (let [rewrite (partial rewrite substitute-uris)]
    (testing "Simple queries are unaltered"
      (let [q "SELECT DISTINCT * WHERE { ?s ?p ?o } OFFSET 0 LIMIT 100"]
        (is-tree= q (rewrite q))))

    (testing "Graph constants are rewritten"
      (is-tree= "SELECT * WHERE {
                 GRAPH <http://foo.com/replaced> {
                   ?s ?p ?o
                 }
                 GRAPH <http://bar.com/replaced> {
                   ?s ?p ?o
                 }
                 GRAPH <http://unaltered.com/> {
                   ?s ?p ?o
                 }
               }"
                (rewrite "SELECT * WHERE {
                 GRAPH <http://foo.com/> {
                   ?s ?p ?o
                 }
                 GRAPH <http://bar.com/> {
                   ?s ?p ?o
                 }
                 GRAPH <http://unaltered.com/> {
                   ?s ?p ?o
                 }
               }"))

      (testing "URI constants in VALUES clauses are rewritten"
        (is-tree= "SELECT * WHERE {
                     VALUES ?g { <http://foo.com/replaced> <http://bar.com/replaced> <http://unaltered.com/> }
                     GRAPH ?g { ?s ?p ?o }
                  }"
                  (rewrite
                   "SELECT * WHERE {
                     VALUES ?g { <http://foo.com/> <http://bar.com/> <http://unaltered.com/> }
                     GRAPH ?g { ?s ?p ?o }
                  }")))

      (testing "Literals are left alone"
        (let [q "SELECT * WHERE {
                     VALUES ?g { <http://foo.com/replaced> <http://bar.com/replaced> <http://unaltered.com/> }
                     GRAPH ?g { ?s ?p \"http://unaltered.com/\" }
                  }"]
          (is-tree= q (rewrite q))))

      (testing "sparql functions"
        (is-tree= "SELECT ?s WHERE {
                     BIND(URI(\"http://foo.com/\") AS ?s)
                  }"
                  (rewrite "SELECT ?s WHERE {
                     BIND(URI(\"http://foo.com/\") AS ?s)
                  }"))))

        ))
