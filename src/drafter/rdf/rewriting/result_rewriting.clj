(ns drafter.rdf.rewriting.result-rewriting
  "The other side of query rewriting; result rewriting.  Result rewriting
  rewrites results and solutions."
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [drafter.util :as util]
            [grafter.rdf.protocols :refer [map->Quad]])
  (:import [org.eclipse.rdf4j.model.impl ContextStatementImpl StatementImpl]
           [org.eclipse.rdf4j.query BooleanQuery GraphQuery TupleQuery TupleQueryResultHandler]
           [org.eclipse.rdf4j.query.impl BindingImpl MapBindingSet]
           org.eclipse.rdf4j.rio.RDFHandler))

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

(defn rewrite-statement [value-mapping statement]
  (map->Quad (util/map-values #(get value-mapping % %) statement)))

(defn- rewrite-value [draft->live value]
  (get draft->live value value))

(defn rewrite-sesame-statement [value-mapping statement]
  (let [subj (rewrite-value value-mapping (.getSubject statement))
            obj (rewrite-value value-mapping (.getObject statement))
            pred (rewrite-value value-mapping (.getPredicate statement))]
        (if-let [graph (rewrite-value value-mapping (.getContext statement))]
          (ContextStatementImpl. subj pred obj graph)
          (StatementImpl. subj pred obj))))

(defn- rewriting-rdf-handler [inner-handler draft->live]
  (reify RDFHandler
    (handleStatement [this statement]
      (.handleStatement inner-handler (rewrite-sesame-statement draft->live statement)))
    (handleNamespace [this prefix uri]
      ;;TODO: are namespaces re-written? Need to re-write results if
      ;;so...
      (.handleNamespace inner-handler prefix uri))
    (startRDF [this] (.startRDF inner-handler))
    (endRDF [this] (.endRDF inner-handler))
    (handleComment [this comment] (.handleComment inner-handler comment))))

;;GraphQuery -> {LiveURI DraftURI} -> GraphQuery
(defn- rewrite-graph-query-results [inner-query live->draft]
  (let [draft->live (set/map-invert live->draft)]
    (reify GraphQuery
      (evaluate [this handler]
        (.evaluate inner-query (rewriting-rdf-handler handler draft->live)))
      (getMaxExecutionTime [this]
        (.getMaxExecutionTime inner-query))
      (setMaxExecutionTime [this max]
        (.setMaxExecutionTime inner-query max)))))

(defn- rewrite-tuple-query-results [inner-query live->draft]
  (let [draft->live (set/map-invert live->draft)]
    (reify TupleQuery
      (evaluate [this handler]
        (.evaluate inner-query (make-select-result-rewriter draft->live handler)))
      (getMaxExecutionTime [this]
        (.getMaxExecutionTime inner-query))
      (setMaxExecutionTime [this max]
        (.setMaxExecutionTime inner-query max)))))

(defn- ->sesame-graph-mapping
  "Rewriting executors store the keys and values in their live -> draft
  graph mapping as java.net.URIs while query rewriting requires them to
  be sesame URI instances. This function maps the mapping from java to sesame
  URIs."
  [uri-mapping]
  (util/map-all util/uri->sesame-uri uri-mapping))

(defn rewrite-query-results [inner-query live->draft]
  (cond
   (instance? GraphQuery inner-query)
   (rewrite-graph-query-results inner-query (->sesame-graph-mapping live->draft))

   (instance? TupleQuery inner-query)
   (rewrite-tuple-query-results inner-query (->sesame-graph-mapping live->draft))

   (instance? BooleanQuery inner-query)
   inner-query))
