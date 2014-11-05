(ns drafter.rdf.sparql-rewriting
  (:require
   [grafter.rdf.sesame :as ses]
   [grafter.rdf :refer [prefixer]]
   [clojure.tools.logging :as log])
  (:import [org.openrdf.model Statement Value Resource Literal URI BNode ValueFactory]
           [org.openrdf.model.impl CalendarLiteralImpl ValueFactoryImpl URIImpl
            BooleanLiteralImpl LiteralImpl IntegerLiteralImpl NumericLiteralImpl
            StatementImpl BNodeImpl ContextStatementImpl]
           [org.openrdf.query QueryLanguage Update Query]
           [org.openrdf.repository.sail SailUpdate]
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
  bound in Graph position.")
  (flatten-ast [query]))

(extend-protocol ISparqlAst
  Query
  (vars-in-graph-position [this]
    (vars-in-graph-position (.getParsedQuery this)))

  ParsedQuery
  (vars-in-graph-position [this]
    (vars-in-graph-position (.getTupleExpr this)))

  (flatten-ast [query-ast]
    (->> query-ast
         .getTupleExpr
         flatten-ast))

  TupleExpr
  (vars-in-graph-position [this]
    (vars-in-graph-position* this))

  (flatten-ast [te]
    (->> te
         drafter.rdf.ClojureCollector/process
         ;; use distinct to preserve order of nodes instead of (into #{} ...)
         distinct))

  SailUpdate
  (vars-in-graph-position [this]
    (vars-in-graph-position (.getParsedUpdate this)))

  (flatten-ast [query-ast]
    (->> query-ast
         .getParsedUpdate
         flatten-ast))

  ParsedUpdate
  (vars-in-graph-position [this]
    ;; TODO find out in what situation
    ;; there may be multiple
    ;; UpdateExprs.  I'm assuming there
    ;; will only ever be one.

    (first (map vars-in-graph-position (.getUpdateExprs this))))

  (flatten-ast [query-ast]
    (->> query-ast
         .getUpdateExprs
         first
         flatten-ast))

  UpdateExpr
  (vars-in-graph-position [this]
    (vars-in-graph-position* this))

  (flatten-ast [ue]
    (->> ue
         drafter.rdf.ClojureCollector/process
         ;; use distinct to preserve order of nodes instead of (into #{} ...)
         distinct)))

(defn rewrite-graph-constants
  ([query-ast graph-map]
     (rewrite-graph-constants query-ast graph-map (filter #(.isConstant %)
                                                          (vars-in-graph-position query-ast))))

  ([query-ast graph-map context-set]
     (doseq [context context-set]
       (when (.isConstant context)
         (when-let [new-uri (get graph-map (.getValue context))]
           (log/debug "Rewriting constant " context " with new-uri " new-uri)
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

(defn compose-graph-replacer [sparql-function-uri query-ast]
  ;; todo do this for each node.
  (let [qnodes (filter (partial instance? IRIFunction)
                       (flatten-ast query-ast))]
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
        (log/debug "Rewriten SPARQL Query AST: " prepared-query))

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
    (let [binding-set (.getBindings preped-update)
          query-ast (-> preped-update .getParsedUpdate .getUpdateExprs first)
          vars-in-graph-position (vars-in-graph-position query-ast)]
      (do
        ;; NOTE the AST is mutable and referenced from the prepared-query
        (compose-graph-replacer (pmdfunctions "replace-live-graph-uri") query-ast)

        (when vars-in-graph-position
          (rewrite-graph-constants query-ast graph-substitutions vars-in-graph-position)
          (log/debug "Rewriten SPARQL Update AST: " preped-update))

        preped-update)))

  preped-update)
