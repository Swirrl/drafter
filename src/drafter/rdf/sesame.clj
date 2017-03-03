(ns drafter.rdf.sesame
  (:require [drafter.rdf.draft-management.jobs :as jobs]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.repository :as repo]
            [drafter.rdf.rewriting.arq :refer [sparql-string->arq-query]]
            [drafter.backend.protocols :refer [->sesame-repo]]
            [clojure.tools.logging :as log])
  (:import [org.openrdf.rio Rio]
           [org.openrdf.query TupleQuery BooleanQuery GraphQuery Update]
           [org.openrdf.rio Rio RDFHandler]
           [org.openrdf.repository Repository]
           [org.openrdf.repository.sparql SPARQLRepository]
           [org.openrdf.query Dataset TupleQueryResultHandler]
           [org.openrdf.query.resultio QueryResultIO]))

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
    (repo/make-restricted-dataset :default-graph graph-restrictions
                                    :named-graphs graph-restrictions)))

(defn apply-restriction [pquery restriction]
  (let [dataset (restricted-dataset restriction)]
    (.setDataset pquery dataset)
    pquery))

(defmulti exec-sesame-prepared-update (fn [repo prepare-fn] (class repo)))
(defmethod exec-sesame-prepared-update SPARQLRepository [repo prepare-fn]
  ;;default sesame implementation executes UPDATE queries in a
  ;;transaction which the remote SPARQL client does not like
  (with-open [conn (repo/->connection repo)]
    (let [pquery (prepare-fn conn)]
      (repo/evaluate pquery))))

(defmethod exec-sesame-prepared-update Repository [repo prepare-fn]
  (with-open [conn (repo/->connection repo)]
    (repo/with-transaction conn
      (let [pquery (prepare-fn conn)]
        (repo/evaluate pquery)))))

(defn execute-restricted-update [backend update-query restrictions]
  (exec-sesame-prepared-update
   (->sesame-repo backend)
   (fn [conn]
     (repo/prepare-update conn update-query (restricted-dataset restrictions)))))

(defn execute-update [backend update-query]
  (execute-restricted-update backend update-query nil))

(defn- exec-ask-query [writer pquery]
  (let [result (.evaluate pquery)]
       (doto writer
         (.handleBoolean result))))

(defn- exec-tuple-query [writer pquery]
  (log/debug "pquery (default) is " pquery " writer is " writer)
  (.evaluate pquery writer))

(defn- exec-graph-query [writer pquery]
  (log/debug "pquery is " pquery " writer is " writer)
  (.evaluate pquery writer))

(defn create-tuple-query-writer [os result-format]
  (QueryResultIO/createWriter result-format os))

(defn create-construct-query-writer [os result-format]
  (Rio/createWriter result-format os))

(defn create-rdf-writer [pquery output-stream result-format]
  (case (get-query-type pquery)
    :select (create-tuple-query-writer output-stream result-format)
    :construct (create-construct-query-writer output-stream result-format)
    (IllegalArgumentException. "Query must be either a SELECT or CONSTRUCT query.")))

(defn signalling-tuple-query-handler [signalling-queue writer]
  (reify TupleQueryResultHandler
    (startQueryResult [this binding-names]
      (.add signalling-queue :ok)
      (.startQueryResult writer binding-names))
    (endQueryResult [this]
      (.endQueryResult writer))
    (handleSolution [this binding-set]
      (.handleSolution writer binding-set))
    (handleBoolean [this b]
      (.handleBoolean writer b))
    (handleLinks [this link-urls]
      (.handleLinks writer link-urls))))

(defn signalling-rdf-handler [signalling-queue handler]
  (reify RDFHandler
    (startRDF [this]
      (.add signalling-queue :ok)
      (.startRDF handler))
    (endRDF [this]
      (.endRDF handler))
    (handleNamespace [this prefix uri]
      (.handleNamespace handler prefix uri))
    (handleStatement [this s]
      (.handleStatement handler s))
    (handleComment [this comment] (.handleComment comment))))

(defn create-signalling-query-handler [pquery output-stream result-format signalling-queue]
  (case (get-query-type pquery)
    :select (signalling-tuple-query-handler signalling-queue (create-tuple-query-writer output-stream result-format))
    :construct (signalling-rdf-handler signalling-queue (create-construct-query-writer output-stream result-format))
    (IllegalArgumentException. "Query must be either a SELECT or CONSTRUCT query.")))

(defn create-query-executor [result-format pquery]
  (case (get-query-type pquery)
    :select (fn [os]
              (let [w (QueryResultIO/createWriter result-format os)]
                (exec-tuple-query w pquery)))

    :ask (fn [os]
           (let [w (QueryResultIO/createWriter result-format os)]
             (exec-ask-query w pquery)))

    :construct (fn [os]
                 (let [w (Rio/createWriter result-format os)]
                   (exec-graph-query w pquery)))
    (throw (IllegalArgumentException. (str "Invalid query type")))))
