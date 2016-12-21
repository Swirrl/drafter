(ns drafter.rdf.rewriting.arq
  "Library of functions for syntactically rewriting queries using Jena ARQ.  Of
  particular interest are sse-zipper and apply-rewriter."
  (:require [clojure.zip :as z])
  (:import [org.apache.jena.query QueryFactory Query Syntax]
           [org.apache.jena.sparql.sse SSE Item ItemList]
           [org.apache.jena.sparql.algebra Op OpAsQuery Algebra]
           [org.apache.jena.graph NodeFactory Node Node_URI]))

(defprotocol ToArqQuery
  (sparql-string->arq-query [this]
    "Converts this into an ARC AST"))

(extend-protocol ToArqQuery
  String
  (sparql-string->arq-query [s]
    (QueryFactory/create s Syntax/syntaxSPARQL_11))

  Query
  (sparql-string->arq-query [q]
    q))

(defprotocol ToSparqlString
  (->sparql-string [this]
    "Attempts to convert this into a string representing a SPARQL query."))

(extend-protocol ToSparqlString
  String
  (->sparql-string [s]
    s)

  Item
  (->sparql-string [i]
    (-> i str SSE/parseOp ->sparql-string))

  Op
  (->sparql-string [o]
    (-> o OpAsQuery/asQuery ->sparql-string))

  Query
  (->sparql-string [q]
    (str q)))

(defprotocol ToSSE
  (->sse-item [this]
    "Attempts to convert this into a Sparql S Expression Item"))

(extend-protocol ToSSE
  String
  (->sse-item [s]
    (SSE/parseItem s))

  Item
  (->sse-item [i]
    i)

  Op
  (->sse-item [o]
    (-> o str ->sse-item))

  Query
  (->sse-item [q]
    (-> q Algebra/compile ->sse-item)))

(defn- sse-list-item? [i]
  (and (instance? Item i)
       (.isList i)))

(defn sse-zipper
  "Construct a zipper on a Sparql SExpression Item, that allows easy persistent
  walking of the sparql algebra tree."
  [sse-item]
  (z/zipper sse-list-item?
            #(-> % .getList seq)
            (fn [node children]
              (let [i (Item/createList)
                    l (.getList i)]
                (doseq [c children]
                  (.add l c))
                i))
            sse-item))

(defn- uri-str->item [uri]
  (Item/createNode uri))

(defn- apply-rewriter-to-describe-uris
  "Special case for rewriting of primitive DESCRIBE :uri queries."
  [rewriter uris]
  (map (fn [uri]
         (str "<" (-> uri
                      uri-str->item
                      sse-zipper
                      rewriter)
              "> ")) uris))

(defn apply-rewriter
  "Applies a rewriter function to the supplied SPARQL query string and returns a
  SPARQL query string.

  The rewriter function is a function from Zipper[Item] -> Item, where Item is a Jena
  ARQ SSE (Sparql S-Expression) representing the original SPARQL query.

  The apply-rewriter function is necessary as it restores information not stored
  in the SSE algebra tree, such as the set of prefixes and the query type."
  [rewriter qstr]

  (let [q (sparql-string->arq-query qstr)
        transform-query (fn [q]
                          (-> q
                              ->sse-item
                              sse-zipper
                              rewriter
                              str
                              (SSE/parseOp (.getPrefixMapping q))
                              OpAsQuery/asQuery))]

    (doto (cond
            ;; NOTE that to support primitive DESCRIBE queries such as those of
            ;; the form "DESCRIBE <uri>" we have to do some additional work as
            ;; Jena returns an #<Item (null)> when converted into an SSE.
            ;;
            ;; So as a hack we convert every URI into an SSE and apply the
            ;; rewriter to them.  The advantage of this is that apply-rewriter
            ;; which is concerned with applying the transformation in the
            ;; context doesn't need to know about the URI mapping (if any), and
            ;; should in theory be more generic.

            (and (.isDescribeType q) (seq (.getResultURIs q)))
            (sparql-string->arq-query
             (apply str "DESCRIBE " (apply-rewriter-to-describe-uris rewriter (.getResultURIs q))))

            ;; This case is distinct from the above as queries like "DESCRIBE ?s
            ;; WHERE { ?s ?p ?o }" have an SSE representation.
            (.isDescribeType q)
            (doto (transform-query q)
              (.setQueryDescribeType))

            (.isSelectType q)
            (let [t (transform-query q)]
              (doseq [uri (.getNamedGraphURIs q)]
                (.addNamedGraphURI t uri))
              (doseq [uri (.getGraphURIs q)]
                (.addGraphURI t uri))
              t)

            (.isConstructType q)
            (doto (transform-query q)
              (.setQueryConstructType)
              (.setConstructTemplate (.getConstructTemplate q)))

            (.isAskType q)
            (doto (transform-query q)
              (.setQueryAskType)))
      (.setPrefixMapping (.getPrefixMapping q)))))

(comment

  ;; Playing with zippers

  (let [item (->sse-item (sparql-string->arq-query "SELECT * WHERE { GRAPH <http://replace-me.com> { ?s ?p ?o } GRAPH <http://bar.com> { ?s ?p ?o } }"))]
    (-> item
        sse-zipper
        z/down
        z/right
        z/down
        z/right
        ;; z/node

        ;;(z/replace (NodeFactory/createURI "http://baz.com/"))
        (z/edit (fn [n] (prn (.getNode n)) (NodeFactory/createURI "http://baz.com/")))

        clojure.zip/root
        str
        SSE/parseOp
        OpAsQuery/asQuery))

;; constructs are different...
(str ( (query->rewritable "PREFIX : <http://foo> CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . ?s2 ?p ?o2 }") identity))

;; NOTE it returns a SELECT... weird... I think it's because select/construct/ask is decided outside the algebra
;; => "PREFIX  :     <http://foo>\n\nSELECT  *\nWHERE\n  { ?s ?p ?o .\n    ?s2 ?p ?o2\n  }\n"

;; https://jena.apache.org/documentation/query/algebra.html

;; round tripping from sparql string -> sse algebra -> sparql string
(-> (QueryFactory/create "PREFIX dcterms: <http://purl.org/dc/terms/> SELECT * WHERE { ?s ?p ?o }")
    Algebra/compile
    OpAsQuery/asQuery)

(-> (QueryFactory/create "PREFIX dcterms: <http://purl.org/dc/terms/>   SELECT * WHERE { ?s ?p ?o }")
    Algebra/compile
    OpAsQuery/asQuery
    str
    SSE/parseItem
    .getList
    seq)

(QueryFactory/create "PREFIX : <http://foo> CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . ?s2 ?p ?o2 }")

(Algebra/compile (QueryFactory/create "SELECT ?s WHERE { GRAPH <:foo> { ?s ?p ?o } GRAPH <:foo> { ?s ?p ?o } }"))

(Algebra/optimize (Algebra/compile (QueryFactory/create "SELECT ?s WHERE { GRAPH <:foo> { ?s ?p ?o } GRAPH <:foo> { ?s ?p ?o } }")))

(Algebra/compile (QueryFactory/create "SELECT ?s WHERE { GRAPH <:foo> { ?s ?p ?o } GRAPH <:foo> { ?s ?p ?o } }"))

(last (.getList (SSE/parseItem "(graph :foo (bgp (triple ?s ?p ?o)))")))
)
