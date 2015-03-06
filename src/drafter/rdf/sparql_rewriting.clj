(ns drafter.rdf.sparql-rewriting
  (:require
   [grafter.rdf.repository :as repo]
   [grafter.rdf :refer [prefixer]]
   [drafter.util :refer [map-values]]
   [drafter.rdf.draft-management :as mgmt]
   [drafter.rdf.sparql-protocol :refer [result-handler-wrapper]]
   [clojure.set :as set]
   [clojure.tools.logging :as log])
  (:import [org.openrdf.model Statement Value Resource Literal URI BNode ValueFactory]
           [org.openrdf.model.impl CalendarLiteralImpl ValueFactoryImpl URIImpl
            BooleanLiteralImpl LiteralImpl IntegerLiteralImpl NumericLiteralImpl
            StatementImpl BNodeImpl ContextStatementImpl]
           [org.openrdf.query QueryLanguage Update Query QueryResultHandler TupleQueryResultHandler BindingSet Binding]
           [org.openrdf.query.impl BindingImpl MapBindingSet]
           [org.openrdf.repository.sail SailUpdate]
           [org.openrdf.query.parser QueryParserUtil ParsedQuery ParsedUpdate ParsedGraphQuery ParsedTupleQuery ParsedBooleanQuery]
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

(defprotocol ISparqlAst
  (->root-node [query]))

(extend-protocol ISparqlAst
  Query
  (->root-node [this] (.getParsedQuery this))

  ;getTupleExpr :: ParsedQuery -> TupleExpr
  ParsedQuery
  (->root-node [this] (->root-node (.getTupleExpr this)))

  ;TupleExpr extends QueryModelNode
  TupleExpr
  (->root-node [this] this)

  ;getParsedUpdate :: SailUpdate -> ParsedUpdate
  SailUpdate
  (->root-node [this] (->root-node (.getParsedUpdate this)))

  ;getUpdateExprs :: ParsedUpdate -> List<UdateExpr>
  ParsedUpdate
    ;; TODO find out in what situation
    ;; there may be multiple
    ;; UpdateExprs.  I'm assuming there
    ;; will only ever be one.
  (->root-node [this]
    (->root-node (first (.getUpdateExprs this))))

  ;UpdateExpr extends QueryModelNode
  UpdateExpr
  (->root-node [this] this))

(defn flatten-ast [ast-like]
  (->> (->root-node ast-like)
       drafter.rdf.ClojureCollector/process
       ;; use distinct to preserve order of nodes instead of (into #{} ...)
       distinct))

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

(defn get-query-nodes-of-type [type query-ast]
  (filter #(instance? type %) (flatten-ast query-ast)))

;rewrite-binding-set-assignments :: ISparqlAst -> Map[URI, URI] -> ()
(defn rewrite-binding-set-assignments!
  "Rewrites all BindingSets in all BindingSetAssignment instances
  inside the given query AST."
  [query-ast graph-map]
  (doseq [bsa (get-query-nodes-of-type BindingSetAssignment query-ast)]
    (update-binding-set-assignment! bsa graph-map)))

(defn rewrite-constant-vars
  "Rewrites the values of any constant Var nodes if they are mapped in
  the given graph mapping."
  [graph-map context-set]
  (doseq [context context-set]
    (when (.isConstant context)
      (when-let [new-uri (get graph-map (.getValue context))]
        (log/info "Rewriting constant " context " with new-uri " new-uri)
        (.setValue context new-uri)))))

(defn rewrite-query-vars!
  "Rewrites all the constant Var nodes in the query AST according to a
  graph mapping."
  [query-ast graph-map]
  (let [vars (get-query-nodes-of-type Var query-ast)]
    (rewrite-constant-vars graph-map vars)))

(defn rewrite-query-value-constants!
  "Rewrites all the ValueConstant nodes in a query AST according to a
  graph mapping."
  [query-ast graph-map]
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
  (let [qnodes (get-query-nodes-of-type IRIFunction query-ast)]
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

(defn rewrite-query-ast! [query-ast graph-map]
  ;; NOTE the AST is mutable and referenced from the prepared-query
  (compose-graph-replacer (pmdfunctions "replace-live-graph-uri") query-ast)
  (rewrite-binding-set-assignments! query-ast graph-map)
  (rewrite-query-vars! query-ast graph-map)
  (rewrite-query-value-constants! query-ast graph-map)
  nil)

(defn rewrite-graph-query [repo query-str graph-map]
  (let [prepared-query (repo/prepare-query repo query-str)
        binding-set (.getBindings prepared-query)
        query-ast (-> prepared-query .getParsedQuery)]
    (rewrite-query-ast! query-ast graph-map)
    prepared-query))

;apply-map-or-default :: Map[a, a] -> (a -> a)
(defn apply-map-or-default
  "Returns a function with a single argument. When applied, it sees if
  the value is a key in the source map - if it exists, the
  corresponding value is returned, otherwise the argument is
  returned."
  [m]
  (fn [r] (get m r r)))

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

;Map[Uri, Uri] -> QueryResultHandler -> QueryResultHandler
(defn- make-select-result-rewriter
  "Creates a new SPARQLResultWriter that rewrites values in solutions
  according to the given graph mapping."
  [graph-map handler]
  (reify
    TupleQueryResultHandler
    (endQueryResult [this]
      (.endQueryResult handler))
    (handleBoolean [this boolean]
      (.handleBoolean handler boolean))
    (handleLinks [this link-urls]
      (.handleLinks handler link-urls))
    (handleSolution [this binding-set]
      (log/debug "select result wrapper " this  "handler: " handler)
      ;; NOTE: mutating the binding set whilst writing (iterating)
      ;; results causes bedlam with the iteration, especially with SPARQL
      ;; DISTINCT queries.
      ;; rewrite-binding-set creates a new BindingSet with the modifications
      (let [new-binding-set (rewrite-binding-set binding-set graph-map)]
        (log/trace "old binding set: " binding-set "new binding-set" new-binding-set)
        (.handleSolution handler new-binding-set)))
    (startQueryResult [this binding-names]
      (.startQueryResult handler binding-names))))

(defn- make-construct-result-rewriter
  "Creates a result-rewriter for construct queries - not a tautology
  honest!"
  [writer draft->live]
  (if (instance? QueryResultHandler writer)
    (result-handler-wrapper writer draft->live)
    writer))

(defn- choose-result-rewriter [query-ast draft->live writer]
  (cond
   (instance? ParsedGraphQuery query-ast) (make-construct-result-rewriter writer draft->live)
   (instance? ParsedTupleQuery query-ast) (make-select-result-rewriter draft->live writer)
   (instance? ParsedBooleanQuery query-ast) writer
   :else writer))

(defn make-draft-query-rewriter [repo draft-uris]
  (let [live->draft (log/spy (mgmt/graph-map repo draft-uris))]
    {:query-rewriter
     (fn [repo query-str]
       (log/info "Using mapping: " live->draft)
       (rewrite-graph-query repo query-str live->draft))

     :result-rewriter
     (fn [prepared-query writer]
       (let [query-ast (.getParsedQuery prepared-query)
             draft->live (set/map-invert live->draft)]
         (choose-result-rewriter query-ast draft->live writer)))
     }))
