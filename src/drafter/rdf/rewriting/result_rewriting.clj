(ns drafter.rdf.rewriting.result-rewriting
  "The other side of query rewriting; result rewriting.  Result rewriting
  rewrites results and solutions."
  (:require
   [grafter.rdf.repository :as repo]
   [grafter.rdf :refer [prefixer]]
   [drafter.rdf.draft-management :as mgmt]
   [clojure.set :as set]
   [clojure.tools.logging :as log])
  (:import [org.openrdf.query GraphQuery BooleanQuery TupleQuery Update QueryResultHandler TupleQueryResultHandler BindingSet Binding]
           [org.openrdf.query.impl BindingImpl MapBindingSet]
           [org.openrdf.rio Rio RDFWriter RDFHandler]
           [org.openrdf.query.algebra.evaluation.function Function FunctionRegistry]
           [org.openrdf.model.impl StatementImpl ContextStatementImpl]))

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

(defn result-handler-wrapper
  ([writer] (result-handler-wrapper writer {}))
  ([writer draft->live]
     (reify
       RDFHandler
       (startRDF [this]
         (.startQueryResult writer '("s" "p" "o")))
       (endRDF [this]
         (.endQueryResult writer))
       (handleNamespace [this prefix uri]
         ;; No op
         )
       (handleStatement [this statement]
         (let [s (.getSubject statement)
               p (.getPredicate statement)
               o (.getObject statement)
               bs (doto (MapBindingSet.)
                    (.addBinding "s" (get draft->live s s))
                    (.addBinding "p" (get draft->live p p))
                    (.addBinding "o" (get draft->live o o)))]
           (.handleSolution writer bs)))
       (handleComment [this comment]
         ;; No op
         ))))

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

(defn- rewrite-value [draft->live value]
  (get draft->live value value))

(defn- rewrite-statement [draft->live statement]
  (let [subj (rewrite-value draft->live (.getSubject statement))
            obj (rewrite-value draft->live (.getObject statement))
            pred (rewrite-value draft->live (.getPredicate statement))]
        (if-let [graph (rewrite-value draft->live (.getContext statement))]
          (ContextStatementImpl. subj pred obj graph)
          (StatementImpl. subj pred obj))))

(defn- rewriting-rdf-handler [inner-handler draft->live]
  (reify RDFHandler
    (handleStatement [this statement]
      (.handleStatement inner-handler (rewrite-statement draft->live statement)))
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
        (.evaluate inner-query (rewriting-rdf-handler handler draft->live))))))

(defn- rewrite-tuple-query-results [inner-query live->draft]
  (let [draft->live (set/map-invert live->draft)]
    (reify TupleQuery
      (evaluate [this handler]
        (.evaluate inner-query (make-select-result-rewriter draft->live handler))))))

(defn rewrite-query-results [inner-query live->draft]
  (cond
   (instance? GraphQuery inner-query)
   (rewrite-graph-query-results inner-query live->draft)

   (instance? TupleQuery inner-query)
   (rewrite-tuple-query-results inner-query live->draft)

   (instance? BooleanQuery inner-query)
   inner-query))
