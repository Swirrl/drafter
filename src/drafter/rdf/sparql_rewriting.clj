(ns drafter.rdf.sparql-rewriting
  (:require
   [grafter.rdf.repository :as repo]
   [grafter.rdf :refer [prefixer]]
   [clojure.set :as set]
   [clojure.tools.logging :as log])
  (:import [org.openrdf.model Statement Value Resource Literal URI BNode ValueFactory]
           [org.openrdf.model.impl CalendarLiteralImpl ValueFactoryImpl URIImpl
            BooleanLiteralImpl LiteralImpl IntegerLiteralImpl NumericLiteralImpl
            StatementImpl BNodeImpl ContextStatementImpl]
           [org.openrdf.query QueryLanguage Update Query]
           [org.openrdf.query.impl BindingImpl MapBindingSet]
           [org.openrdf.repository.sail SailUpdate]
           [org.openrdf.query.parser QueryParserUtil ParsedQuery ParsedUpdate]
           [org.openrdf.queryrender.sparql SPARQLQueryRenderer]
           [org.openrdf.query.algebra.evaluation.function Function]
           [org.openrdf.query.algebra UpdateExpr TupleExpr Var StatementPattern Extension ExtensionElem FunctionCall IRIFunction ValueExpr
            BindingSetAssignment ValueConstant]
           [org.openrdf.query.algebra.helpers QueryModelTreePrinter VarNameCollector StatementPatternCollector QueryModelVisitorBase]))

(def pmdfunctions (prefixer "http://publishmydata.com/def/functions#"))

(defn ->sparql-ast
  "Converts a SPARQL query string into a mutable Sesame SPARQL AST."

  ([query-string]
     (->sparql-ast query-string nil))
  ([query-string base-uri]
     (QueryParserUtil/parseQuery QueryLanguage/SPARQL query-string base-uri)))

(defn- vars-in-graph-position*
  "Given a parsed query Expr and returns the vars which are
  bound in Graph position."
  [expr]
  (reduce (fn [acc val]
            (if-let [var (.getContextVar val)]
              (conj acc var)
              acc))
          [] (StatementPatternCollector/process expr)))

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
           (log/info "Rewriting constant " context " with new-uri " new-uri)
           (.setValue context new-uri))))
     query-ast))

(defn rewrite-binding
  "Rewrites the value of a Binding if it appears in the given graph map"
  [binding graph-map]
  (if-let [mapped-graph (get graph-map (.getValue binding))]
    (BindingImpl. (.getName binding) mapped-graph)
    binding))

;binding-seq->binding-set :: Seq[Binding] -> BindingSet
(defn binding-seq->binding-set
  "Creates a BindingSet instance from a sequence of Bindings"
  [bindings]
  (let [bs (MapBindingSet.)]
    (doseq [b bindings]
      (.addBinding bs b))
    bs))

;rewrite-binding-set :: BindingSet -> Map[URI, URI] -> BindingSet
(defn rewrite-binding-set
  "Creates a new BindingSet where each value is re-written according
  to the given graph mapping."
  [binding-set graph-map]
  (let [mapped-bindings (map #(rewrite-binding % graph-map) binding-set)]
    (binding-seq->binding-set mapped-bindings)))

;update-binding-set-assignment :: BindingSetAssignment -> Map[URI, URI] -> ()
(defn update-binding-set-assignment!
  "Modifies all the BindingSets in a BindingSetAssignment according to
  the given graph mapping."
  [bsa graph-map]
  (let [rewritten-bindings (mapv #(rewrite-binding-set % graph-map) (.getBindingSets bsa))]
    (.setBindingSets bsa rewritten-bindings)))

;rewrite-binding-set-assignments :: ISparqlAst -> Map[URI, URI] -> ()
(defn rewrite-binding-set-assignments!
  "Rewrites all BindingSets in all BindingSetAssignment instances
  inside the given query AST."
  [query-ast graph-map]
  (let [nodes (flatten-ast query-ast)]
    (doseq [bsa (filter #(instance? BindingSetAssignment %) nodes)]
      (update-binding-set-assignment! bsa graph-map))))

(defn get-query-nodes-of-type [type query-ast]
  (filter #(instance? type %) (flatten-ast query-ast)))

(defn get-query-vars [query-ast]
  (get-query-nodes-of-type Var query-ast))

(defn rewrite-query-vars! [query-ast graph-map]
  (let [vars (get-query-vars query-ast)]
    (rewrite-graph-constants query-ast graph-map vars)))

(defn rewrite-query-value-constants! [query-ast graph-map]
  (let [value-constants (get-query-nodes-of-type ValueConstant query-ast)]
    (doseq [c value-constants]
      (when-let [rewritten (get graph-map (.getValue c))]
        (.setValue c rewritten)))))

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
  (let [prepared-query (repo/prepare-query repo query-str)
        binding-set (.getBindings prepared-query)
        query-ast (-> prepared-query .getParsedQuery)
        vars-in-graph-position (vars-in-graph-position query-ast)]
    (do
      ;; NOTE the AST is mutable and referenced from the prepared-query
      (compose-graph-replacer (pmdfunctions "replace-live-graph-uri") query-ast)
      (rewrite-binding-set-assignments! query-ast query-substitutions)
      (rewrite-query-vars! query-ast query-substitutions)
      (rewrite-query-value-constants! query-ast query-substitutions)

      prepared-query)))

;map-values :: (a -> b) -> Map[k, a] -> Map[k, b]
(defn map-values
  "Maps"
  [f m]
  (let [mapped-pairs (map (fn [[k v]] [k (f v)]) m)]
    (into {} mapped-pairs)))

;apply-map-or-default :: Map[a, a] -> (a -> a)
(defn apply-map-or-default
  "Returns a function with a single argument. When applied, it sees if
  the value is a key in the source map - if it exists, the
  corresponding value is returned, otherwise the argument is
  returned."
  [m]
  (fn [r]
    (if-let [mapped (get m r)]
      mapped
      r)))

;rewrite-result :: Map[Uri, Uri] -> Map[a, Uri] -> Map[a, Uri]
(defn rewrite-result
  "Rewrites all the values in a result based on the given draft->live
  graph mapping."
  [graph-map r]
  (map-values (apply-map-or-default graph-map) r))

(defn rewrite-graph-results [query-substitutions prepared-query]
  (let [query-ast (.getParsedQuery prepared-query)]
    (let [result-substitutions (set/map-invert query-substitutions)]
        (->> (repo/evaluate prepared-query)
             (map #(rewrite-result result-substitutions %))))))

(defn evaluate-with-graph-rewriting
  "Rewrites the results in the query."
  ([repo query-str query-substitutions]
       (evaluate-with-graph-rewriting repo query-str query-substitutions nil))
    ([repo query-str query-substitutions dataset]
       (let [prepared-query (doto (rewrite-graph-query repo query-str query-substitutions)
                              (.setDataset dataset))]
         (rewrite-graph-results query-substitutions prepared-query))))
