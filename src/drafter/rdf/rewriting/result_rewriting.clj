(ns drafter.rdf.rewriting.result-rewriting
  "The other side of query rewriting; result rewriting.  Result rewriting
  rewrites results and solutions."
  (:require
   [grafter.rdf.repository :as repo]
   [grafter.rdf :refer [prefixer]]
   [drafter.util :refer [map-values]]
   [drafter.rdf.draft-management :as mgmt]
   [drafter.rdf.rewriting.query-rewriting :refer [rewrite-sparql-string]]
   [drafter.backend.sesame :refer [result-handler-wrapper]]
   [clojure.set :as set]
   [clojure.tools.logging :as log])
  (:import [org.openrdf.query GraphQuery BooleanQuery TupleQuery Update QueryResultHandler TupleQueryResultHandler BindingSet Binding]
           [org.openrdf.query.impl BindingImpl MapBindingSet]
           [org.openrdf.query.algebra.evaluation.function Function FunctionRegistry]))

(defn- rewrite-binding
  "Rewrites the value of a Binding if it appears in the given graph map"
  [binding graph-map]
  (if-let [mapped-graph (get graph-map (.getValue binding))]
    (BindingImpl. (.getName binding) mapped-graph)
    binding))

;binding-seq->binding-set :: Seq[Binding] -> BindingSet
(defn- binding-seq->binding-set
  "Creates a BindingSet instance from a sequence of Bindings"
  [bindings]
  (let [bs (MapBindingSet.)]
    (doseq [b bindings]
      (.addBinding bs b))
    bs))

;rewrite-binding-set :: BindingSet -> Map[URI, URI] -> BindingSet
(defn- rewrite-binding-set
  "Creates a new BindingSet where each value is re-written according
  to the given graph mapping."
  [binding-set graph-map]
  (let [mapped-bindings (map #(rewrite-binding % graph-map) binding-set)]
    (binding-seq->binding-set mapped-bindings)))

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
  (let [result-substitutions (set/map-invert query-substitutions)]
        (->> (repo/evaluate prepared-query)
             (map #(rewrite-result result-substitutions %)))))

(defn evaluate-with-graph-rewriting
  "Rewrites the results in the query."
  ([repo query-str query-substitutions]
   (evaluate-with-graph-rewriting repo query-str query-substitutions nil))
  ([repo query-str query-substitutions dataset]
   (let [rewritten-query (rewrite-sparql-string query-substitutions query-str)
         prepared-query (doto (repo/prepare-query repo rewritten-query)
                          (.setDataset dataset))]
     (rewrite-graph-results query-substitutions prepared-query))))

;Map[Uri, Uri] -> QueryResultHandler -> QueryResultHandler
(defn make-select-result-rewriter
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

(defn make-construct-result-rewriter
  "Creates a result-rewriter for construct queries - not a tautology
  honest!"
  [writer draft->live]
  (if (instance? QueryResultHandler writer)
    (result-handler-wrapper writer draft->live)
    writer))

(defn choose-result-rewriter [query-ast draft->live writer]
  (cond
   (instance? GraphQuery query-ast) (make-construct-result-rewriter writer draft->live)
   (instance? TupleQuery query-ast) (make-select-result-rewriter draft->live writer)
   (instance? BooleanQuery query-ast) writer
   :else writer))
