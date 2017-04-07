(ns drafter.rdf.sesame
  (:require [clojure.tools.logging :as log]
            [drafter.backend.protocols :refer [->sesame-repo]]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.rewriting.arq :refer [sparql-string->arq-query]]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.repository :as repo])
  (:import [org.openrdf.query BooleanQuery Dataset GraphQuery TupleQuery TupleQueryResultHandler Update]
           org.openrdf.query.resultio.QueryResultIO
           org.openrdf.repository.Repository
           org.openrdf.repository.sparql.SPARQLRepository
           [org.openrdf.rio RDFHandler Rio]))

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

(defn validate-query [query-str]
  "Validates a query by parsing it using ARQ. If the query is invalid
  a QueryParseException is thrown."
  (sparql-string->arq-query query-str)
  query-str)

(defn prepare-query [backend sparql-string]
    (let [repo (->sesame-repo backend)
          validated-query-string (validate-query sparql-string)]
      (repo/prepare-query repo validated-query-string)))

(defn- get-restrictions [graph-restrictions]
  (cond
   (coll? graph-restrictions) graph-restrictions
   (fn? graph-restrictions) (graph-restrictions)
   :else nil))

(defn restricted-dataset
  "Returns a restricted dataset or nil when given either a 0-arg
  function or a collection of graph uris."
  [graph-restrictions]
  {:pre [(or (nil? graph-restrictions)
             (coll? graph-restrictions)
             (fn? graph-restrictions))]
   :post [(or (instance? Dataset %)
              (nil? %))]}
  (when-let [graph-restrictions (get-restrictions graph-restrictions)]
    (let [stringified-restriction (map str graph-restrictions)]
      (repo/make-restricted-dataset :default-graph stringified-restriction
                                    :named-graphs stringified-restriction))))

(defn apply-restriction [pquery restriction]
  (let [dataset (restricted-dataset restriction)]
    (.setDataset pquery dataset)
    pquery))

(defn create-tuple-query-writer [os result-format]
  (QueryResultIO/createWriter result-format os))

(defn create-construct-query-writer [os result-format]
  (Rio/createWriter result-format os))

(defn signalling-tuple-query-handler [send-channel writer]
  (reify TupleQueryResultHandler
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

(defn signalling-rdf-handler [send-channel handler]
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
    (handleComment [this comment] (.handleComment comment))))

(defn create-signalling-query-handler [pquery output-stream result-format send-channel]
  (case (get-query-type pquery)
    :select (signalling-tuple-query-handler send-channel (create-tuple-query-writer output-stream result-format))
    :construct (signalling-rdf-handler send-channel (create-construct-query-writer output-stream result-format))
    (IllegalArgumentException. "Query must be either a SELECT or CONSTRUCT query.")))

