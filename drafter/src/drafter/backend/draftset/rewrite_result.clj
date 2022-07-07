(ns drafter.backend.draftset.rewrite-result
  "The other side of query rewriting; result rewriting.  Result rewriting
  rewrites results and solutions."
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :refer [map->Quad]])
  (:import [org.eclipse.rdf4j.model.impl ContextStatementImpl StatementImpl]
           [org.eclipse.rdf4j.query BooleanQuery GraphQuery TupleQuery TupleQueryResultHandler TupleQueryResult GraphQueryResult Dataset Query]
           [org.eclipse.rdf4j.query.impl BindingImpl MapBindingSet SimpleDataset]
           org.eclipse.rdf4j.rio.RDFHandler
           [org.eclipse.rdf4j.model Statement]))

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

(defn- rewrite-dataset
  "Creates a new dataset where the graphs are re-written according to
   the given graph mapping."
  [graph-mapping ^Dataset dataset]
  (let [rewritten-dataset (SimpleDataset.)
        resolve-graph (fn [g] (get graph-mapping g g))]
    (doseq [default-graph (.getDefaultGraphs dataset)]
      (.addDefaultGraph rewritten-dataset (resolve-graph default-graph)))

    (doseq [named-graph (.getNamedGraphs dataset)]
      (.addNamedGraph rewritten-dataset (resolve-graph named-graph)))

    (doseq [remove-graph (.getDefaultRemoveGraphs dataset)]
      (.addDefaultRemoveGraph rewritten-dataset (resolve-graph remove-graph)))

    (when-let [insert-graph (.getDefaultInsertGraph dataset)]
      (.setDefaultInsertGraph rewritten-dataset (resolve-graph insert-graph)))

    rewritten-dataset))

;Map[Uri, Uri] -> QueryResultHandler -> QueryResultHandler
(defn- make-select-result-rewriter
  "Creates a new SPARQLResultWriter that rewrites values in solutions
  according to the given graph mapping."
  [graph-map handler]
  (reify
    TupleQueryResult
    (getBindingNames [this]
      (.getBindingNames handler))

    (close [this]
      (.close handler))

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

(defn rewrite-statement
  "Rewrites the values within a grafter quad according to the given mapping"
  [value-mapping statement]
  (map->Quad (util/map-values #(get value-mapping % %) statement)))

(defn- rewrite-value [draft->live value]
  (get draft->live value value))

(defn rewrite-rdf4j-statement
  "Rewrites the values within a Statement according to the given mapping"
  [value-mapping ^Statement statement]
  (let [subj (rewrite-value value-mapping (.getSubject statement))
        obj (rewrite-value value-mapping (.getObject statement))
        pred (rewrite-value value-mapping (.getPredicate statement))]
    (if-let [graph (rewrite-value value-mapping (.getContext statement))]
      (ContextStatementImpl. subj pred obj graph)
      (StatementImpl. subj pred obj))))

(defn- rewriting-rdf-handler
  "Returns an RDFHandler which re-writes draft values within result statements to their
   corresponding live values before passing them to inner-handler."
  [inner-handler draft->live]
  (reify RDFHandler
    (handleStatement [this statement]
      (.handleStatement inner-handler (rewrite-rdf4j-statement draft->live statement)))
    (handleNamespace [this prefix uri]
      ;;TODO: are namespaces re-written? Need to re-write results if
      ;;so...
      (.handleNamespace inner-handler prefix uri))
    (startRDF [this] (.startRDF inner-handler))
    (endRDF [this] (.endRDF inner-handler))
    (handleComment [this comment] (.handleComment inner-handler comment))))

(defn- rewriting-graph-query-result
  "Returns a GraphQueryResult which rewrites draft values within result binding sets to
   their corresponding live values according to the draft->live mapping"
  [draft->live ^GraphQueryResult result]
  (reify GraphQueryResult
    (getNamespaces [_this]
      (.getNamespaces result))
    (hasNext [_this]
      (.hasNext result))
    (next [_this]
      (let [stmt (.next result)]
        (rewrite-rdf4j-statement draft->live stmt)))
    (remove [_this]
      (.remove result))
    (close [_this]
      (.close result))))

(defn- rewriting-tuple-query-result
  "Returns a TupleQueryResult which rewrites draft values within result binding sets to
   their corresponding live values according to the draft->live mapping"
  [draft->live ^TupleQueryResult result]
  (reify TupleQueryResult
    (getBindingNames [_this]
      (.getBindingNames result))
    (hasNext [_this]
      (.hasNext result))
    (next [_this]
      (let [bs (.next result)]
        (rewrite-binding-set bs draft->live)))
    (remove [_this]
      (.remove result))
    (close [_this]
      (.close result))))

(defmacro def-rewriting-query-record
  "Defines a record with the fields inner-query live->draft and draft->live which implements
   the rdf4j Query interface and rewrites binding and datasets according to the given draftset
   graph mapping. The optional specs should be specified as for defrecord i.e. a sequence of
   interface/protocols and their implementation methods."
  [name & specs]
  (concat
    ['defrecord name '[inner-query live->draft draft->live]]
    '(Query
      (setBinding [_this name value]
        (.setBinding inner-query name (get live->draft value value)))
      (removeBinding [_this name]
        (.removeBinding inner-query name))
      (clearBindings [_this]
        (.clearBindings inner-query))
      (getBindings [_this]
        (rewrite-binding-set (.getBindings inner-query) draft->live))
      (setDataset [_this dataset]
        (.setDataset inner-query (rewrite-dataset live->draft dataset)))
      (getDataset [_this]
        (let [inner-dataset (.getDataset inner-query)]
          (rewrite-dataset draft->live inner-dataset)))
      (getIncludeInferred [this]
        (.getIncludeInferred inner-query))
      (setIncludeInferred [this include-inferred?]
        (.setIncludeInferred inner-query include-inferred?))
      (getMaxExecutionTime [this]
        (.getMaxExecutionTime inner-query))
      (setMaxExecutionTime [this max]
        (.setMaxExecutionTime inner-query max)))
    '(Object
       (toString [_this] (.toString inner-query)))
    specs))

(def-rewriting-query-record RewritingBooleanQuery
  BooleanQuery
  (evaluate [{:keys [inner-query] :as _this}]
    (.evaluate inner-query)))

(def-rewriting-query-record RewritingTupleQuery
  TupleQuery
  (evaluate [{:keys [inner-query draft->live] :as this}]
    (let [inner-result (.evaluate inner-query)]
      (rewriting-tuple-query-result draft->live inner-result)))
  (evaluate [{:keys [inner-query draft->live] :as this} handler]
    (.evaluate inner-query (make-select-result-rewriter draft->live handler))))

(def-rewriting-query-record RewritingGraphQuery
  GraphQuery
  (evaluate [{:keys [inner-query draft->live] :as this}]
    (let [inner-result (.evaluate inner-query)]
      (rewriting-graph-query-result draft->live inner-result)))
  (evaluate [{:keys [inner-query draft->live]} handler]
    (.evaluate inner-query (rewriting-rdf-handler handler draft->live))))

(defn- ->sesame-graph-mapping
  "Rewriting executors store the keys and values in their live -> draft
  graph mapping as java.net.URIs while query rewriting requires them to
  be sesame URI instances. This function maps the mapping from java to sesame
  URIs."
  [uri-mapping]
  (util/map-all util/uri->sesame-uri uri-mapping))

(defprotocol ToRewritingQuery
  (->rewriting-query [this live->draft draft->live]))

(extend-protocol ToRewritingQuery
  GraphQuery
  (->rewriting-query [this live->draft draft->live]
    (->RewritingGraphQuery this live->draft draft->live))

  TupleQuery
  (->rewriting-query [this live->draft draft->live]
    (->RewritingTupleQuery this live->draft draft->live))

  BooleanQuery
  (->rewriting-query [this live->draft draft->live]
    (->RewritingBooleanQuery this live->draft draft->live)))

(defn rewriting-query
  "Returns a query of the same type as which rewrites bindings, dataset and query results
   according to the given live->draft graph mapping"
  [inner-query live->draft]
  (let [live->draft (->sesame-graph-mapping live->draft)
        draft->live (set/map-invert live->draft)]
    (->rewriting-query inner-query live->draft draft->live)))
