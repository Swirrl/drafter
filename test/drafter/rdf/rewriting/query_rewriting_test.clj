(ns drafter.rdf.rewriting.query-rewriting-test
  (:require [clojure
             [string :refer [trim]]
             [test :refer :all]]
            [drafter.rdf.rewriting
             [arq :refer :all]
             [query-rewriting :refer :all]]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :each validate-schemas)

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

(def substitute-uris (partial uri-constant-rewriter substitutions))

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
        .hasOffset                 "SELECT * WHERE { ?s ?p ?o } OFFSET 10"
        .hasDatasetDescription     "SELECT * FROM <http://foo.com> WHERE { ?s ?p ?o }"
        .hasDatasetDescription     "SELECT * FROM NAMED <http://foo.com> WHERE { ?s ?p ?o }"
        .isAskType                 "ASK WHERE { ?s ?p ?o }"
        .isConstructType           "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
        .getConstructTemplate      "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"

        has-prefix-mapping?        "PREFIX foo: <http://foo.com> CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
        has-prefix-mapping?        "PREFIX foo: <http://foo.com> SELECT * WHERE { ?s ?p ?o }"
        has-prefix-mapping?        "PREFIX foo: <http://foo.com> ASK WHERE { ?s ?p ?o }"
        has-prefix-mapping?        "PREFIX foo: <http://foo.com> DESCRIBE <http://foo.com>")

      (are [query-str]
        (= {"foo" "http://foo.com" "bar" "http://bar.com"}
           (into {} (.getNsPrefixMap (.getPrefixMapping (rewrite->query query-str)))))

        "PREFIX foo: <http://foo.com> PREFIX bar: <http://bar.com> SELECT * WHERE { foo:baz ?p ?o }"
        "PREFIX foo: <http://foo.com> PREFIX bar: <http://bar.com> ASK { foo:baz ?p ?o }"
        "PREFIX foo: <http://foo.com> PREFIX bar: <http://bar.com> CONSTRUCT { foo:boo bar:baz ?p} WHERE { foo:baz bar:baz ?o }"
        "PREFIX foo: <http://foo.com> PREFIX bar: <http://bar.com> DESCRIBE foo:bar"))))

(deftest primitive-describe-rewriting
  (let [rewritten-describe (trim (str (apply-rewriter substitute-uris
                                                      "DESCRIBE <http://foo.com/> <http://bar.com/> <http://unreplaced.com/>")))]
    (is (= "DESCRIBE <http://foo.com/replaced> <http://bar.com/replaced> <http://unreplaced.com/>" rewritten-describe))))

(deftest uri-rewriter-test
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
               }")))

    (testing "Rewrites subqueries"
      (is-tree= "SELECT (COUNT(*) as ?count) {
                   SELECT DISTINCT ?uri ?graph WHERE {
                     GRAPH <http://foo.com/replaced> {
                       ?uri ?p ?o .
                     }
                   }
                 }"
                (rewrite "SELECT (COUNT(*) as ?count) {
                   SELECT DISTINCT ?uri ?graph WHERE {
                     GRAPH <http://foo.com/> {
                       ?uri ?p ?o .
                     }
                   }
                 }")))

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
                  }")))

    (testing "uri rewriting"
      (is-tree= "PREFIX qb: <http://purl.org/linked-data/cube#>
                 SELECT ?obs
                 WHERE {
                   ?obs a qb:Observation ;
                          qb:measureType ?measure ;
                          ?measure ?value ;
                          .
                 }
                 GROUP BY ?obs
                 HAVING (COUNT(?value) > 1)"

                (rewrite "PREFIX qb: <http://purl.org/linked-data/cube#>
                          SELECT ?obs
                          WHERE {
                            ?obs a qb:Observation ;
                                   qb:measureType ?measure ;
                                   ?measure ?value ;
                                   .
                          }
                          GROUP BY ?obs
                          HAVING (COUNT(?value) > 1)")))

    (testing "JENA-1566"
      (is-tree= "SELECT ?b WHERE { VALUES ?b { true }}"
                (rewrite "SELECT ?b WHERE { VALUES ?b { true }}")))))
