(ns drafter.backend.draftset.rewrite-result
  "The other side of query rewriting; result rewriting.  Result rewriting
  rewrites results and solutions."
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :refer [map->Quad]])
  (:import [org.eclipse.rdf4j.model.impl ContextStatementImpl StatementImpl]
           [org.eclipse.rdf4j.query BooleanQuery GraphQuery TupleQuery TupleQueryResultHandler TupleQueryResult GraphQueryResult Dataset]
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

;;GraphQuery -> {LiveURI DraftURI} -> GraphQuery
(defn- rewriting-graph-query
  "Returns a GraphQuery which rewrites bindings, datasets and query results according to
   the given live->draft graph mapping"
  [inner-query live->draft]
  (let [draft->live (set/map-invert live->draft)]
    (reify GraphQuery
      (evaluate [_this]
        (let [inner-result (.evaluate inner-query)]
          (rewriting-graph-query-result draft->live inner-result)))
      (evaluate [this handler]
        (.evaluate inner-query (rewriting-rdf-handler handler draft->live)))
      (setBinding [_this name value]
        (.setBinding inner-query name (get live->draft value value)))
      (removeBinding [_this name]
        (.removeBinding inner-query name))
      (clearBindings [_this]
        (.clearBindings inner-query))
      (getBindings [this]
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
        (.setMaxExecutionTime inner-query max)))))

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

(defn- rewriting-tuple-query
  "Returns a TupleQuery which rewrites bindings, datasets and query results according to
   the given live->draft graph mapping."
  [inner-query live->draft]
  (let [draft->live (set/map-invert live->draft)]
    (reify TupleQuery
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
      (evaluate [_this]
        (let [inner-result (.evaluate inner-query)]
          (rewriting-tuple-query-result draft->live inner-result)))
      (evaluate [this handler]
        (.evaluate inner-query (make-select-result-rewriter draft->live handler)))
      (getIncludeInferred [this]
        (.getIncludeInferred inner-query))
      (setIncludeInferred [this include-inferred?]
        (.setIncludeInferred inner-query include-inferred?))
      (getMaxExecutionTime [this]
        (.getMaxExecutionTime inner-query))
      (setMaxExecutionTime [this max]
        (.setMaxExecutionTime inner-query max)))))

(defn- rewriting-boolean-query
  "Returns a BooleanQuery which rewrites bindings, datasets and query results according to
   the given live->draft graph mapping."
  [inner-query live->draft]
  (let [draft->live (set/map-invert live->draft)]
    (reify BooleanQuery
      (evaluate [_this]
        (.evaluate inner-query))
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
        (.setMaxExecutionTime inner-query max)))))

(defn- ->sesame-graph-mapping
  "Rewriting executors store the keys and values in their live -> draft
  graph mapping as java.net.URIs while query rewriting requires them to
  be sesame URI instances. This function maps the mapping from java to sesame
  URIs."
  [uri-mapping]
  (util/map-all util/uri->sesame-uri uri-mapping))

(defn rewriting-query
  "Returns a query of the same type as which rewrites bindings, dataset and query results
   according to the given live->draft graph mapping"
  [inner-query live->draft]
  (cond
   (instance? GraphQuery inner-query)
   (rewriting-graph-query inner-query (->sesame-graph-mapping live->draft))

   (instance? TupleQuery inner-query)
   (rewriting-tuple-query inner-query (->sesame-graph-mapping live->draft))

   (instance? BooleanQuery inner-query)
   (rewriting-boolean-query inner-query (->sesame-graph-mapping live->draft))))
