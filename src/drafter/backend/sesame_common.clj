(ns drafter.backend.sesame-common
  (:require [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add statements]]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [drafter.backend.protocols :as backend]
            [drafter.write-scheduler :as scheduler]
            [swirrl-server.async.jobs :refer [create-job create-child-job]]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.draft-management :as mgmt]
            [grafter.rdf.protocols :as proto]
            [drafter.rdf.rewriting.query-rewriting :refer [rewrite-sparql-string]]
            [drafter.rdf.rewriting.result-rewriting :refer [choose-result-rewriter result-handler-wrapper]]
            [drafter.rdf.rewriting.arq :refer [->sparql-string sparql-string->arq-query]]
            [drafter.util :refer [construct-dynamic*]])
  (:import [org.openrdf.query TupleQuery TupleQueryResult
            TupleQueryResultHandler BooleanQueryResultHandler
            BindingSet QueryLanguage BooleanQuery GraphQuery Update]
           [org.openrdf.rio Rio RDFWriter RDFHandler]
           [org.openrdf.query.resultio QueryResultWriter]
           [org.openrdf.rio.ntriples NTriplesWriter]
           [org.openrdf.rio.nquads NQuadsWriter]
           [org.openrdf.rio.n3 N3Writer]
           [org.openrdf.rio.n3 N3Writer]
           [org.openrdf.rio.trig TriGWriter]
           [org.openrdf.rio.trix TriXWriter]
           [org.openrdf.rio.turtle TurtleWriter]
           [org.openrdf.rio.rdfxml RDFXMLWriter]
           [org.openrdf.query.parser ParsedBooleanQuery ParsedGraphQuery ParsedTupleQuery]
           [org.openrdf.query.resultio TupleQueryResultFormat]
           [org.openrdf.query.resultio.text BooleanTextWriter]
           [org.openrdf.query.resultio.sparqljson SPARQLResultsJSONWriter]
           [org.openrdf.query.resultio.sparqlxml SPARQLResultsXMLWriter SPARQLBooleanXMLWriter]
           [org.openrdf.query.resultio.binary BinaryQueryResultWriter]
           [org.openrdf.query.resultio.text.csv SPARQLResultsCSVWriter]
           [org.openrdf.query.resultio.text.tsv SPARQLResultsTSVWriter]
           [org.openrdf.query Dataset]
           [org.openrdf.query.impl MapBindingSet]))

(defn restricted-dataset
  "Returns a restricted dataset or nil when given either a 0-arg
  function or a collection of graph uris."
  [graph-restrictions]
  {:pre [(or (nil? graph-restrictions)
             (coll? graph-restrictions)
             (fn? graph-restrictions))]
   :post [(or (instance? Dataset %)
              (nil? %))]}
  (let [graph-restrictions (cond
                            (coll? graph-restrictions) graph-restrictions
                            (fn? graph-restrictions) (graph-restrictions)
                            :else nil)]

    (when graph-restrictions
      (repo/make-restricted-dataset :default-graph graph-restrictions
                                    :named-graphs graph-restrictions))))

;class->writer-fn :: Class[T] -> (OutputStream -> T)
(defn class->writer-fn [writer-class]
  (fn [output-stream]
    (construct-dynamic* writer-class output-stream)))

(defn negotiate-content-writer
  "Given a prepared query and a mime-type return the appropriate
  Sesame SPARQLResultsWriter class, according to the SPARQL protocols
  content negotiation rules."
  [preped-query format]
  (get (condp instance? preped-query
         TupleQuery   { "application/sparql-results+json" SPARQLResultsJSONWriter
                        "application/sparql-results+xml" SPARQLResultsXMLWriter
                        "application/x-binary-rdf" BinaryQueryResultWriter
                        "text/csv" SPARQLResultsCSVWriter
                        "text/tab-separated-values" SPARQLResultsTSVWriter
                        "text/html" SPARQLResultsCSVWriter
                        "text/plain" SPARQLResultsCSVWriter
                        }
         BooleanQuery { "application/sparql-results+xml" SPARQLResultsXMLWriter
                        "application/sparql-results+json" SPARQLResultsJSONWriter
                        "application/x-binary-rdf" BinaryQueryResultWriter
                        "text/plain" BooleanTextWriter
                        "text/html" BooleanTextWriter
                        }
         GraphQuery   {
                       "application/n-triples" NTriplesWriter
                       "application/n-quads" NQuadsWriter
                       "text/n3" N3Writer
                       "application/trig" TriGWriter
                       "application/trix" TriXWriter
                       "text/turtle" TurtleWriter
                       "text/html" TurtleWriter
                       "application/rdf+xml" RDFXMLWriter
                       "text/csv" SPARQLResultsCSVWriter
                       "text/tab-separated-values" SPARQLResultsTSVWriter
                       }
         nil) format))

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

(defn- get-graph-query-handler [writer]
  (if (instance? QueryResultWriter writer)
    (result-handler-wrapper writer)
    writer))

(defn- exec-graph-query [writer pquery result-notify-fn]
  (log/debug "pquery is " pquery " writer is " writer)
  (let [handler (get-graph-query-handler writer)
        notifying-handler (notifying-rdf-handler result-notify-fn handler)]
    (.evaluate pquery handler)))

(defn- get-exec-query [pquery writer-fn]
  (fn [ostream notifier-fn]
    (let [writer (writer-fn ostream)]
      (cond
       (instance? BooleanQuery pquery)
       (exec-ask-query writer pquery notifier-fn)

       (instance? TupleQuery pquery)
       (exec-tuple-query writer pquery notifier-fn)

       :else
       (exec-graph-query writer pquery notifier-fn)))))

(defn- get-repo [this] (:repo this))

(defn validate-query [query-str]
  "Validates a query by parsing it using ARQ. If the query is invalid
  a QueryParseException is thrown."
  (sparql-string->arq-query query-str)
  query-str)

(defn- prepare-query [repo sparql-string graph-restrictions]
    (let [validated-query-string (validate-query sparql-string)
          dataset (restricted-dataset graph-restrictions)
          pquery (repo/prepare-query repo validated-query-string)]
      (.setDataset pquery dataset)
      pquery))

(defn- get-query-type [pquery]
    (condp instance? pquery
      TupleQuery :select
      BooleanQuery :ask
      GraphQuery :construct
      Update :update
      nil))

(defn negotiate-result-writer [pquery media-type]
    (if-let [writer-class (negotiate-content-writer pquery media-type)]
      (class->writer-fn writer-class)))

(defn- exec-update [repo update-query restrictions]
  (with-open [conn (repo/->connection repo)]
      (let [dataset (restricted-dataset restrictions)
            pquery (repo/prepare-update conn update-query dataset)]
        (repo/with-transaction conn
          (repo/evaluate pquery)))))

(defn- make-draft-query-rewriter
  "Build both a query rewriter and an accompanying result rewriter tied together
  in a hash-map, for supplying to our draft SPARQL endpoints as configuration."

  [live->draft]
  {:query-rewriter (fn [query] (rewrite-sparql-string live->draft query))
   :result-rewriter
   (fn [prepared-query writer]
     (let [draft->live (set/map-invert live->draft)]
       (choose-result-rewriter prepared-query draft->live writer))
     )})

(defrecord RewritingSesameSparqlExecutor [inner live->draft]
  backend/SparqlExecutor
  (prepare-query [_ sparql-string restrictions]
    (let [rewritten-query (rewrite-sparql-string live->draft sparql-string)]
      (backend/prepare-query inner rewritten-query restrictions)))

  (get-query-type [_ pquery]
    (backend/get-query-type inner pquery))

  (negotiate-result-writer [_ prepared-query media-type]
    (if-let [inner-writer-fn (backend/negotiate-result-writer inner prepared-query media-type)]
      (fn [ostream]
        (let [draft->live (set/map-invert live->draft)
              writer (inner-writer-fn ostream)]
          (choose-result-rewriter prepared-query draft->live writer)))))

  (create-query-executor [_ writer-fn pquery]
    (backend/create-query-executor inner writer-fn pquery)))

(def default-to-connection-impl
  {:->connection (comp repo/->connection get-repo)})

(def default-triple-readable-impl
  {:to-statements (fn [this options]
                    (proto/to-statements (get-repo this) options))})

(def default-sparqlable-impl
  {:query-dataset (fn [this sparql-string model]
                    (proto/query-dataset (get-repo this) sparql-string model))})

(def default-isparql-updatable-impl
  {:update! (fn [this sparql-string]
              (proto/update! (get-repo this) sparql-string))})

(def default-sparql-query-impl
  {:prepare-query (fn [this sparql-string restrictions]
                    (prepare-query (get-repo this) sparql-string restrictions))

   :get-query-type (fn [_ prepared-query] (get-query-type prepared-query))
   
   :negotiate-result-writer (fn [_ prepared-query media-type]
                              (negotiate-result-writer prepared-query media-type))

   :create-query-executor (fn [_ writer-fn prepared-query]
                            (get-exec-query prepared-query writer-fn))})

(def default-query-rewritable-impl
  {:create-rewriter ->RewritingSesameSparqlExecutor})

(def default-sparql-update-impl
  {:execute-update (fn [this query-string restrictions]
                     (exec-update (get-repo this) query-string restrictions))})

(def default-stoppable-impl
  {:stop (comp repo/shutdown get-repo)})

(defn- append-data-in-batches [repo draft-graph metadata triples job]
  (jobs/with-job-exception-handling job
    (let [conn (repo/->connection repo)
          [current-batch remaining-triples] (split-at jobs/batched-write-size triples)]

      (log/info (str "Adding a batch of triples to repo" current-batch))
      (backend/append-data-batch! repo draft-graph current-batch)

      (if-not (empty? remaining-triples)
        ;; resubmit the remaining batches under the same job to the
        ;; queue to give higher priority jobs a chance to write
        (let [apply-next-batch (partial append-data-in-batches
                                        repo draft-graph metadata remaining-triples)]
          (scheduler/queue-job! (create-child-job job apply-next-batch)))

        (do
          (mgmt/add-metadata-to-graph conn draft-graph metadata)
          (log/info (str "File import (append) to draft-graph: " draft-graph " completed"))

          (jobs/job-succeeded! job))))))

(defn- append-data-to-graph-job
  "Return a job function that adds the triples from the specified file
  to the specified graph.

  This operation is batched at the :batch-write level to allow
  cooperative scheduling with :sync-writes.

  It works by concatenating the existing live quads with a lazy-seq on
  the uploaded file.  This combined lazy sequence is then split into
  the current batch and remaining, with the current batch being
  applied before the job is resubmitted (under the same ID) with the
  remaining triples.

  The last batch is finally responsible for signaling job completion
  via a side-effecting call to complete-job!"

  [backend draft-graph tempfile rdf-format metadata]

  (let [new-triples (statements tempfile
                                :format rdf-format
                                :buffer-size jobs/batched-write-size)

        ;; NOTE that this is technically not transactionally safe as
        ;; sesame currently only supports the READ_COMMITTED isolation
        ;; level.
        ;;
        ;; As there is no read lock or support for (repeatable reads)
        ;; this means that the CONSTRUCT below can witness data
        ;; changing underneath it.
        ;;
        ;; TODO: protect against this, either by adopting a better
        ;; storage engine or by adding code to either refuse make-live
        ;; operations on jobs that touch the same graphs that we're
        ;; manipulating here, or to raise an error on the batch task.
        ;;
        ;; I think the newer versions of Sesame 1.8.x might also provide better
        ;; support for different isolation levels, so we might want to consider
        ;; upgrading.
        ;;
        ;; http://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Read_committed
        ;;
        ;; This can occur if a user does a make-live on a graph
        ;; which is being written to in a batch job.
    ]

    (jobs/make-job :batch-write [job]
              (append-data-in-batches backend draft-graph metadata new-triples job))))

;;draft API
(def default-api-operations-impl
  {:new-draft-job (fn [this live-graph-uri params]
                    (jobs/create-draft-job (get-repo this) live-graph-uri params))
   
   :append-data-to-graph-job append-data-to-graph-job

   :copy-from-live-graph-job (fn [this draft-graph-uri]
                               (jobs/create-copy-from-live-graph-job (get-repo this) draft-graph-uri))
   
   :migrate-graphs-to-live-job (fn [this graphs]
                                 (jobs/migrate-graph-live-job (get-repo this) graphs))
   :delete-metadata-job (fn [this graphs meta-keys]
                          (jobs/create-delete-metadata-job (get-repo this) graphs meta-keys))
   
   :update-metadata-job (fn [this graphs metadata]
                          (jobs/create-update-metadata-job (get-repo this) graphs metadata))
   
   :delete-graph-job (fn [this graph contents-only?]
                       (log/info "Starting batch deletion job")
                       (create-job :batch-write
                                   (partial jobs/delete-in-batches
                                            (get-repo this)
                                            graph
                                            contents-only?)))})

(defn- append-data-batch [backend graph-uri triple-batch]
  (let [repo (get-repo backend)]
    (with-open [conn (repo/->connection repo)]
      (repo/with-transaction conn
        (add conn graph-uri triple-batch)))))

(defn- append-graph-metadata [backend graph-uri metadata]
  (let [repo (get-repo backend)]
    ;;TODO: Update in transaction?
    (doseq [[meta-name value] metadata]
      (mgmt/upsert-single-object! repo graph-uri meta-name value))))

;;draft management
(def default-draft-management-impl
  {:append-data-batch! append-data-batch
   :append-graph-metadata! append-graph-metadata})
