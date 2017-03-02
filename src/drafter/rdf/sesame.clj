(ns drafter.rdf.sesame
  (:require [drafter.rdf.draft-management.jobs :as jobs]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.repository :as repo]
            [drafter.rdf.rewriting.arq :refer [sparql-string->arq-query]]
            [drafter.backend.protocols :refer [->sesame-repo]]
            [clojure.tools.logging :as log])
  (:import [org.openrdf.rio Rio]
           [org.openrdf.query TupleQuery TupleQueryResultHandler
                              BooleanQuery GraphQuery Update]
           [org.openrdf.rio Rio RDFHandler]
           [org.openrdf.repository Repository]
           [org.openrdf.repository.sparql SPARQLRepository]           
           [org.openrdf.query Dataset]
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
  (case (get-query-type pquery)
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
