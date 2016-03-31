(ns drafter.rdf.sesame
  (:require [drafter.rdf.draft-management.jobs :as jobs]
            [grafter.rdf :refer [statements]]
            [clojure.tools.logging :as log])
  (:import [java.util ArrayList]
           [org.openrdf.rio Rio]
           [org.openrdf.rio.helpers StatementCollector]
           [org.openrdf.query TupleQuery TupleQueryResult
            TupleQueryResultHandler BooleanQueryResultHandler
            BindingSet QueryLanguage BooleanQuery GraphQuery Update]
           [org.openrdf.rio Rio RDFWriter RDFHandler]
           [org.openrdf.query.resultio QueryResultWriter QueryResultIO]
           [org.openrdf.model.impl ContextStatementImpl]))

(defn is-quads-format? [rdf-format]
  (.supportsContexts rdf-format))

(defn is-triples-format? [rdf-format]
  (not (is-quads-format? rdf-format)))

(defn read-statements
  "Creates a lazy stream of statements from an input stream containing
  RDF data serialised in the given format."
  ([input rdf-format] (read-statements input rdf-format jobs/batched-write-size))
  ([input rdf-format batch-size]
   (statements input
               :format rdf-format
               :buffer-size batch-size)))

(defn get-query-type
  "Returns a keyword indicating the type query represented by the
  given prepared query. Returns nil if the query could not be
  classified."
  [backend pquery]
  (condp instance? pquery
      TupleQuery :select
      BooleanQuery :ask
      GraphQuery :construct
      Update :update
      nil))

(defn notifying-query-result-handler [notify-fn inner-handler]
  (reify
    TupleQueryResultHandler
    (handleBoolean [this b]
      (notify-fn)
      (.handleBoolean inner-handler b))
    (handleLinks [this links] (.handleLinks inner-handler links))
    (startQueryResult [this binding-names] (.startQueryResult inner-handler binding-names))
    (endQueryResult [this] (.endQueryResult inner-handler))
    (handleSolution [this binding-set]
      (notify-fn)
      (.handleSolution inner-handler binding-set))))

(defn notifying-rdf-handler [notify-fn inner-handler]
  (reify
    RDFHandler
    (startRDF [this]
      (.startRDF inner-handler))
    (endRDF [this]
      (.endRDF inner-handler))
    (handleNamespace [this prefix uri]
      (.handleNamespace inner-handler prefix uri))
    (handleStatement [this statement]
      (notify-fn)
      (.handleStatement inner-handler statement))
    (handleComment [this comment]
      (.handleComment inner-handler comment))))

(defn- exec-ask-query [writer pquery result-notify-fn]
  (let [notifying-handler (notifying-query-result-handler result-notify-fn writer)
           result (.evaluate pquery)]
       (doto notifying-handler
         (.handleBoolean result))))

(defn- exec-tuple-query [writer pquery result-notify-fn]
  (log/debug "pquery (default) is " pquery " writer is " writer)
  (.evaluate pquery (notifying-query-result-handler result-notify-fn writer)))

(defn- exec-graph-query [writer pquery result-notify-fn]
  (log/debug "pquery is " pquery " writer is " writer)
  (let [notifying-handler (notifying-rdf-handler result-notify-fn writer)]
    (.evaluate pquery notifying-handler)))

(defn create-query-executor [backend result-format pquery]
  (case (get-query-type backend pquery)
    :select (fn [os notifier-fn]
              (let [w (QueryResultIO/createWriter result-format os)]
                (exec-tuple-query w pquery notifier-fn)))

    :ask (fn [os notifier-fn]
           (let [w (QueryResultIO/createWriter result-format os)]
             (exec-ask-query w pquery notifier-fn)))

    :construct (fn [os notifier-fn]
                 (let [w (Rio/createWriter result-format os)]
                   (exec-graph-query w pquery notifier-fn)))
    (throw (IllegalArgumentException. (str "Invalid query type")))))
