(ns drafter.rdf.sparql-rewriting
  (:require
   [grafter.rdf.sesame :as ses]
   [grafter.rdf :refer [prefixer]]
            [taoensso.timbre :as timbre])
  (:import [org.openrdf.model Statement Value Resource Literal URI BNode ValueFactory]
           [org.openrdf.model.impl CalendarLiteralImpl ValueFactoryImpl URIImpl
            BooleanLiteralImpl LiteralImpl IntegerLiteralImpl NumericLiteralImpl
            StatementImpl BNodeImpl ContextStatementImpl]
           [org.openrdf.query QueryLanguage Update Query]
           [org.openrdf.query.parser QueryParserUtil ParsedQuery ParsedUpdate]
           [org.openrdf.queryrender.sparql SPARQLQueryRenderer]
           [org.openrdf.query.algebra.evaluation.function Function]
           [org.openrdf.query.algebra UpdateExpr TupleExpr Var StatementPattern Extension ExtensionElem FunctionCall IRIFunction ValueExpr]
           [org.openrdf.query.algebra.helpers QueryModelTreePrinter VarNameCollector StatementPatternCollector QueryModelVisitorBase]))

(def pmdfunctions (prefixer "http://publishmydata.com/def/functions#"))

(defn ->sparql-ast
  "Converts a SPARQL query string into a mutable Sesame SPARQL AST."

  ([query-string]
     (->sparql-ast query-string nil))
  ([query-string base-uri]
     (QueryParserUtil/parseQuery QueryLanguage/SPARQL query-string base-uri)))

(defn- vars-in-graph-position*
  "Given a parsed query Expr and returns the set of Vars which are
  bound in Graph position."
  [expr]
  (reduce (fn [acc val]
            (if-let [var (.getContextVar val)]
              (conj acc var)
              acc))
          #{} (StatementPatternCollector/process expr)))

(defprotocol ISparqlAst
  ;; TODO extend this with other ops - perhaps more generic ones.
  (vars-in-graph-position [query]
    "Given a parsed query, context-set returns the set of Vars which are
  bound in Graph position."))

(extend-protocol ISparqlAst
  Query
  (vars-in-graph-position [this]
    (vars-in-graph-position (.getParsedQuery this)))

  ParsedQuery
  (vars-in-graph-position [this]
    (vars-in-graph-position (.getTupleExpr this)))

  TupleExpr
  (vars-in-graph-position [this]
    (vars-in-graph-position* this))

  Update
  (vars-in-graph-position [this]
    (vars-in-graph-position (.getParsedUpdate this)))

  ParsedUpdate
  (vars-in-graph-position [this]
                                 ;; TODO find out in what situation
                             ;; there may be multiple
                             ;; UpdateExprs.  I'm assuming there
                             ;; will only ever be one.

    (map vars-in-graph-position (.getUpdateExprs this)))

  UpdateExpr
  (vars-in-graph-position [this]
    (vars-in-graph-position* this)))

(defn rewrite-graph-constants
  ([query-ast graph-map]
     (rewrite-graph-constants query-ast graph-map (filter #(.isConstant %)
                                                          (vars-in-graph-position query-ast))))

  ([query-ast graph-map context-set]
     (doseq [context context-set]
       (when (.isConstant context)
         (let [new-uri (get graph-map (.getValue context))]
           (timbre/info "Rewriting constant " context " with new-uri " new-uri)
           (.setValue context new-uri))))
     query-ast))

(def function-registry (org.openrdf.query.algebra.evaluation.function.FunctionRegistry/getInstance))

(defn register-function
  "Register an arbitrary custom function with the global SPARQL engine registry."
  [function-registry uri f]
  (io!
   (let [sesame-f (drafter.rdf.SesameFunction. uri f)]
     (doto function-registry
       (.add sesame-f)))))

(defn replace-values [uri-map]
  "Return a function that replaces arbitrary values supplied values.
  If a value is not replaced the original value is returned."
  (fn [arg]
    (get uri-map arg arg)))

(defn ->sparql-string
  "Converts a SPARQL AST back into a query string."
  [query-ast]
  (.render (SPARQLQueryRenderer.)
           query-ast))

(defn flatten-query-ast
  "Flattens the SPARQL AST into a clojure sequence for filtering etc."
  [query-ast]
  (->> query-ast
       .getTupleExpr
       drafter.rdf.ClojureCollector/process
       ;; use distinct to preserve order of nodes instead of (into #{} ...)
       distinct))

(defn compose-graph-replacer [sparql-function-uri query-ast]
  ;; todo do this for each node.
  (let [qnodes (filter (partial instance? IRIFunction)
                       (flatten-query-ast query-ast))]
    (doseq [qnode qnodes]
      (let [parent (.getParentNode qnode)
            replacement-node (FunctionCall. sparql-function-uri (into-array ValueExpr  [(.clone qnode)]))]
        (.replaceWith qnode
                      replacement-node)))
    query-ast))

(defn substitute-results [substitution-map graph-var-names result]
  (reduce (fn [acc-res var-name]
            (let [original (get result var-name)]
              (assoc acc-res var-name
                     (get substitution-map original))))
          result
          graph-var-names))

(defn rewrite-graph-query [repo query-str query-substitutions]
  (let [prepared-query (ses/prepare-query repo query-str)
        binding-set (.getBindings prepared-query)
        query-ast (-> prepared-query .getParsedQuery)
        vars-in-graph-position (vars-in-graph-position query-ast)]
    (do
      ;; NOTE the AST is mutable and referenced from the prepared-query
      (compose-graph-replacer (pmdfunctions "replace-live-graph-uri") query-ast)

      (when vars-in-graph-position
        (rewrite-graph-constants query-ast query-substitutions vars-in-graph-position)
        (timbre/info "Rewriten query AST: " prepared-query))

      prepared-query)))

(defn rewrite-graph-results [query-substitutions prepared-query]
  (let [query-ast (.getParsedQuery prepared-query)]

    (if-let [vars-in-graph-position (seq (vars-in-graph-position query-ast))]
      (let [;; filter out constant values in graph position, leaving just "variables"
            unbound-var-names (filter (complement #(.isConstant %)) vars-in-graph-position)
            graph-var-names (map #(.getName %)
                                 unbound-var-names)
            result-substitutions (clojure.set/map-invert query-substitutions)]

        (->> (ses/evaluate prepared-query)
             (map (partial substitute-results result-substitutions graph-var-names))))
      (ses/evaluate prepared-query))))

(defn evaluate-with-graph-rewriting
  "Rewrites the results in the query."
  ([repo query-str query-substitutions]
       (evaluate-with-graph-rewriting repo query-str query-substitutions nil))
    ([repo query-str query-substitutions dataset]
       (let [prepared-query (doto (rewrite-graph-query repo query-str query-substitutions)
                              (.setDataset dataset))]
         (rewrite-graph-results query-substitutions prepared-query))))

(defn rewrite-update-request [preped-update graph-substitutions]
  (when (vars-in-graph-position preped-update)
    (rewrite-graph-query preped-update graph-substitutions)
      (timbre/info "Rewritten update to: " preped-update))

  preped-update)

(comment




  (evaluate-with-graph-rewriting drafter.handler/repo
                           "SELECT * WHERE {
                               BIND(URI(\"http://opendatacommunities.org/my-graph\") AS ?g)
                               GRAPH ?g {
                                 ?s ?p ?o
                              }
                            } LIMIT 10"
                           {(URIImpl. "http://opendatacommunities.org/my-graph") (URIImpl. "http://publishmydata.com/graphs/drafter/draft/ccb30b57-2b67-4e7b-a8a8-1a00aa2eeaf1")} nil)



  (->sparql-ast "SELECT ?g WHERE {
                   BIND(uri(concat(\"http://live-graph.com/\",\"/graph1\")) AS ?g) .
                   GRAPH ?g {
                     ?s ?p ?o .
                   }
                 }")

  (ses/query drafter.handler/repo "SELECT * WHERE { GRAPH <http://opendatacommunities.org/my-graph> { ?s ?p ?o }} LIMIT 1")

  (register-function function-registry "http://publishmydata.com/def/functions#replace-live-graph-uri" (fn [x] (URIImpl. "http://publishmydata.com/graphs/drafter/draft/ccb30b57-2b67-4e7b-a8a8-1a00aa2eeaf1")))
  (ses/query drafter.handler/repo "PREFIX drafter: <http://publishmydata.com/def/functions#> SELECT ?result WHERE { BIND( drafter:foo(10) AS ?result )}")

  (rewrite-graph-constants (->sparql-ast "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } GRAPH <http://live-graph.com/graph1> { ?s ?p2 ?o2 }}")
                  {"http://live-graph.com/graph1" "http://draft-graph.com/graph1"})

  ;; =>
  ;; select ?g ?s ?p ?o ?p2 ?o2
  ;; where
  ;; {
  ;;  GRAPH ?g {
  ;;   ?s ?p ?o.
  ;; } GRAPH <http://draft-graph.com/graph1> {
  ;;   ?s ?p2 ?o2.
  ;; }
  ;;}

  (context-set (->sparql-ast "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } GRAPH <http://live-graph.com/graph1> { ?s ?p2 ?o2 }}"))
  ;; => #{"g" "http://live-graph.com/graph1"}


  )

(comment
  (take 10 (ses/query drafter.handler/repo "SELECT * WHERE { ?s ?p ?o }" :default-graph ["http://publishmydata.com/graphs/drafter/draft/ccb30b57-2b67-4e7b-a8a8-1a00aa2eeaf1"] :union-graph ["http://publishmydata.com/graphs/drafter/draft/ccb30b57-2b67-4e7b-a8a8-1a00aa2eeaf1"]))

  )

(comment

  (ses/query drafter.handler/repo "SELECT ?g WHERE {
BIND(uri(concat(\"http://opendatacommunities.org\",\"/my-graph\")) AS ?pmd-test) .
BIND(uri(concat(\"http://opendatacommunities.org\",\"/my-graph\")) AS ?g) . GRAPH ?g { ?s ?p ?o .}}")

  )

(comment

  (take 10 (ses/evaluate (ses/prepare-query drafter.handler/repo "SELECT * WHERE { ?s ?p ?o }")))



  ;; Parsing a Query
  (parse-query "SELECT * WHERE { ?s ?p ?o }")

  ;; gets all bound variables
  (-> (parse-query "SELECT * WHERE { ?s ?p ?o OPTIONAL { ?g ?z ?a }  }") .getTupleExpr .getBindingNames) ;; #{?s ?p ?o ?g ?z ?a}

  (-> (parse-query "SELECT * WHERE { ?s ?p ?o OPTIONAL { ?g ?z ?a }  }") .getTupleExpr .getAssuredBindingNames) ;; #{?s ?p ?o}



  )
