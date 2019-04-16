(ns drafter.rdf.sesame
  (:require [clojure.tools.logging :as log]
            [drafter.backend.common :refer [->sesame-repo]]
            [drafter.backend.draftset.arq :refer [sparql-string->arq-query]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [grafter-2.rdf4j.io :refer [statements]]
            [grafter-2.rdf4j.repository :as repo])
  (:import [org.eclipse.rdf4j.query BindingSet BooleanQuery Dataset GraphQuery TupleQuery TupleQueryResultHandler TupleQueryResult Update]
           [org.eclipse.rdf4j.query.resultio QueryResultIO QueryResultWriter]
           org.eclipse.rdf4j.repository.Repository
           org.eclipse.rdf4j.repository.sparql.SPARQLRepository
           [org.eclipse.rdf4j.rio RDFHandler Rio]))

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
  [pquery]
  (condp instance? pquery
      TupleQuery :select
      BooleanQuery :ask
      GraphQuery :construct
      Update :update
      nil))







(defn create-tuple-query-writer [os result-format]
  (QueryResultIO/createWriter result-format os))

(defn create-construct-query-writer [os result-format]
  (Rio/createWriter result-format os))

(defn signalling-tuple-query-handler [send-channel ^BindingSet binding-set ^QueryResultWriter writer]
  (reify
    TupleQueryResult
    (getBindingNames [this]
      (let [is (iterator-seq (.iterator binding-set))]
        (if (seq is)
          []
          is)))

    (close [this]
      ;; no-op can't call .close on writer's
      )

    TupleQueryResultHandler
    (startQueryResult [this binding-names]
      (send-channel)
      (.startQueryResult writer binding-names))
    (endQueryResult [this]
      (.endQueryResult writer))
    (handleSolution [this binding-set]
      (.handleSolution writer binding-set))
    (handleBoolean [this b]
      (.handleBoolean writer b))
    (handleLinks [this link-urls]
      (.handleLinks writer link-urls))))

(defn signalling-rdf-handler [send-channel ^RDFHandler handler]
  (reify RDFHandler
    (startRDF [this]
      (send-channel)
      (.startRDF handler))
    (endRDF [this]
      (.endRDF handler))
    (handleNamespace [this prefix uri]
      (.handleNamespace handler prefix uri))
    (handleStatement [this s]
      (.handleStatement handler s))
    (handleComment [this comment]
      (.handleComment handler comment))))

(defn create-signalling-query-handler [pquery output-stream result-format send-channel]
  (case (get-query-type pquery)
    :select (signalling-tuple-query-handler send-channel (.getBindings pquery) (create-tuple-query-writer output-stream result-format))
    :construct (signalling-rdf-handler send-channel (create-construct-query-writer output-stream result-format))
    (throw (IllegalArgumentException. "Query must be either a SELECT or CONSTRUCT query."))))