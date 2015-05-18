(ns drafter.rdf.sparql-rewriting
  (:require
   [grafter.rdf.repository :as repo]
   [grafter.rdf :refer [prefixer]]
   [drafter.util :refer [map-values]]
   [drafter.rdf.draft-management :as mgmt]
   [drafter.rdf.sparql-protocol :refer [result-handler-wrapper]]
   [clojure.set :as set]
   [clojure.tools.logging :as log])
  (:import [org.openrdf.query GraphQuery BooleanQuery TupleQuery Update QueryResultHandler TupleQueryResultHandler BindingSet Binding]
           [org.openrdf.query.impl BindingImpl MapBindingSet]
           [org.openrdf.query.algebra.evaluation.function Function FunctionRegistry]
           [drafter.rdf URIMapper Rewriters]
           [com.hp.hpl.jena.query QueryFactory Syntax]))

(def pmdfunctions (prefixer "http://publishmydata.com/def/functions#"))

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

(defn sparql-string->ast [query-str]
  (QueryFactory/create query-str Syntax/syntaxSPARQL_11))

;->sparql-string :: AST -> String
(defn ->sparql-string
  "Converts a SPARQL AST back into a query string."
  [query-ast]
  (.serialize query-ast Syntax/syntaxSPARQL_11))

;rewrite-query-ast :: AST -> AST
(defn rewrite-query-ast
  "Rewrites a query AST according to the given live->draft graph
  mapping."
  [query-ast graph-map]
  (let [uri-mapper (URIMapper/create graph-map)]
    (.rewrite Rewriters/queryRewriter uri-mapper query-ast)))

;rewrite-sparql-string :: Map[Uri, Uri] -> String -> String
(defn rewrite-sparql-string
  "Parses a SPARQL query string, rewrites it according to the given
  live->draft graph mapping and then returns the re-written query
  serialised as a string."
  [live->draft query-str]
  (log/info "Rewriting query " query-str)
  (log/info "Mapping: " live->draft)
  
  (let [query-ast (sparql-string->ast query-str)
        rewritten-ast (rewrite-query-ast query-ast live->draft)
        rewritten-query (->sparql-string rewritten-ast)]
    (log/info "Re-written query: " rewritten-query)
    rewritten-query))

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
   (instance? GraphQuery query-ast) (make-construct-result-rewriter writer draft->live)
   (instance? TupleQuery query-ast) (make-select-result-rewriter draft->live writer)
   (instance? BooleanQuery query-ast) writer
   :else writer))

(def ^{:doc "The global function registry for drafter SPARQL functions."}
  function-registry (FunctionRegistry/getInstance))

(defn register-function!
  "Register an arbitrary custom function with the global SPARQL engine registry."
  [function-registry uri f]
  (io!
   (let [sesame-f (drafter.rdf.SesameFunction. uri f)]
     (doto function-registry
       (.add sesame-f)))))

(defn make-draft-query-rewriter [live->draft]
  {:query-rewriter (fn [query] (rewrite-sparql-string live->draft query))

   :result-rewriter
   (fn [prepared-query writer]
     (let [draft->live (set/map-invert live->draft)]
       (choose-result-rewriter prepared-query draft->live writer)))})
