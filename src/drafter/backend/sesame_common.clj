(ns drafter.backend.sesame-common
  (:require [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add statements]]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [drafter.common.api-routes :refer [meta-params]]
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

(defn- negotiate-content-writer
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

(defn- negotiate-result-writer [pquery media-type]
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
    (let [[current-batch remaining-triples] (split-at jobs/batched-write-size triples)]

      (log/info (str "Adding a batch of triples to repo" current-batch))
      (backend/append-data-batch! repo draft-graph current-batch)

      (if-not (empty? remaining-triples)
        ;; resubmit the remaining batches under the same job to the
        ;; queue to give higher priority jobs a chance to write
        (let [apply-next-batch (partial append-data-in-batches
                                        repo draft-graph metadata remaining-triples)]
          (scheduler/queue-job! (create-child-job job apply-next-batch)))

        (do
          (backend/append-graph-metadata! repo draft-graph metadata)
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

(defn- migrate-graph-to-live!
  "Moves the triples from the draft graph to the draft graphs live destination."
  [db draft-graph-uri]

  (if-let [live-graph-uri (mgmt/lookup-live-graph db draft-graph-uri)]
    (do
      ;;DELETE the target (live) graph and copy the draft to it
      ;;TODO: Use MOVE query?
      (mgmt/delete-graph-contents! db live-graph-uri)

      (let [contents (repo/query db
                            (str "CONSTRUCT { ?s ?p ?o } WHERE
                                 { GRAPH <" draft-graph-uri "> { ?s ?p ?o } }"))

            ;;If the source (draft) graph is empty then the migration
            ;;is a deletion. If it is the only draft of the live graph
            ;;then all references to the live graph are being removed
            ;;from the data. In this case the reference to the live
            ;;graph should be removed from the state graph. Note this
            ;;case and use it when cleaning up the state graph below.
            is-only-draft? (not (mgmt/has-more-than-one-draft? db live-graph-uri))]

        ;;if the source (draft) graph has data then copy it to the live graph and
        ;;make it public.
        (if (not (empty? contents))
          (do
            (add db live-graph-uri contents)
            (mgmt/set-isPublic! db live-graph-uri true)))

        ;;delete draft data
        (mgmt/delete-graph-contents! db draft-graph-uri)

        ;;NOTE: At this point all the live and draft graph data has
        ;;been updated: the live graph contents match those of the
        ;;published draft, and the draft data has been deleted.

        ;;Clean up the state graph - all references to the draft graph should always be removed.
        ;;The live graph should be removed if the draft was empty (operation was a deletion) and
        ;;it was the only draft of the live graph

        ;;WARNING: Draft graph state must be deleted before the live graph state!
        ;;DELETE query depends on the existence of the live->draft connection in the state
        ;;graph
        (mgmt/delete-draft-graph-state! db draft-graph-uri)

        (if (and is-only-draft? (empty? contents))
          (mgmt/delete-live-graph-from-state! db live-graph-uri)))
      
      (log/info (str "Migrated graph: " draft-graph-uri " to live graph: " live-graph-uri)))

    (throw (ex-info (str "Could not find the live graph associated with graph " draft-graph-uri)
                    {:error :graph-not-found}))))

(defn- migrate-graphs-to-live! [backend graphs]
  (log/info "Starting make-live for graphs " graphs)
  (with-open [conn (repo/->connection (get-repo backend))]
    (repo/with-transaction conn
      (doseq [g graphs]
        (migrate-graph-to-live! conn g))))
  (log/info "Make-live for graphs " graphs " done"))

(defn- migrate-graphs-to-live-job [backend graphs]
  (jobs/make-job :exclusive-write [job]
                 (migrate-graphs-to-live! backend graphs)
                 (jobs/job-succeeded! job)))

(defn- new-draft-job [backend live-graph params]
  (jobs/make-job :sync-write [job]
            (with-open [conn (repo/->connection (get-repo backend))]
              (let [draft-graph-uri (repo/with-transaction conn
                                      (mgmt/create-managed-graph! conn live-graph)
                                      (mgmt/create-draft-graph! conn live-graph (meta-params params)))]
                (jobs/job-succeeded! job {:guri draft-graph-uri})))))

(defprotocol SesameBatchOperations
  (delete-graph-batch! [this graph-uri batch-size]
    "Deletes batch-size triples from the given graph uri. Most sesame
    backends delete graphs in batches rather than DROPping the target
    graph since it may be slow. The default delete graph job deletes
    the source graph in batches so backends wishing to use the default
    implementation should also implement this protocol."))

(def default-sesame-batch-operations-impl
  {:delete-graph-batch! (fn [backend graph-uri batch-size]
                            (with-open [conn (repo/->connection (get-repo backend))]
                              (repo/with-transaction conn
                                (mgmt/delete-graph-batched! conn graph-uri batch-size))))})

(defn- finish-delete-job! [backend graph contents-only? job]
  (when-not contents-only?
    (mgmt/delete-draft-graph-state! backend graph))
  (jobs/job-succeeded! job))

(defn- delete-in-batches [backend graph contents-only? job]
  ;; Loops until the graph is empty, then deletes state graph if not a
  ;; contents-only? deletion.
  ;;
  ;; Checks that graph is a draft graph - will only delete drafts.
  (if (and (mgmt/graph-exists? backend graph)
           (mgmt/draft-exists? backend graph))
      (do
        (delete-graph-batch! backend graph jobs/batched-write-size)

        (if (mgmt/graph-exists? backend graph)
          ;; There's more graph contents so queue another job to continue the
          ;; deletions.
          (let [apply-next-batch (partial delete-in-batches backend graph contents-only?)]
            (scheduler/queue-job! (create-child-job job apply-next-batch)))
          (finish-delete-job! backend graph contents-only? job)))
      (finish-delete-job! backend graph contents-only? job)))

(defn- delete-graph-job [this graph-uri contents-only?]
  (log/info "Starting batch deletion job")
  (create-job :batch-write
              (partial delete-in-batches this graph-uri contents-only?)))

;;draft API
(def default-api-operations-impl
  {:new-draft-job new-draft-job
   
   :append-data-to-graph-job append-data-to-graph-job

   :copy-from-live-graph-job (fn [this draft-graph-uri]
                               (jobs/create-copy-from-live-graph-job (get-repo this) draft-graph-uri))
   
   :migrate-graphs-to-live-job migrate-graphs-to-live-job
   
   :delete-metadata-job (fn [this graphs meta-keys]
                          (jobs/create-delete-metadata-job (get-repo this) graphs meta-keys))
   
   :update-metadata-job jobs/create-update-metadata-job
   
   :delete-graph-job delete-graph-job})

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

(defn- get-all-drafts [backend]
  (with-open [conn (repo/->connection (get-repo backend))]
    (mgmt/query-all-drafts conn)))

;;draft management
(def default-draft-management-impl
  {:append-data-batch! append-data-batch
   :append-graph-metadata! append-graph-metadata
   :get-all-drafts get-all-drafts
   :migrate-graphs-to-live! migrate-graphs-to-live!
   :get-live-graph-for-draft (fn [this draft-graph-uri]
                               (with-open [conn (repo/->connection (get-repo this))]
                                 (mgmt/get-live-graph-for-draft conn)))})

(defn- add-statement-impl
  ([this statement] (proto/add-statement (get-repo this) statement))
  ([this graph statement] (proto/add-statement (get-repo this) graph statement)))

(defn- add-impl
  ([this triples] (proto/add (get-repo this) triples))
  ([this graph triples] (proto/add (get-repo this) graph triples))
  ([this graph format triple-stream] (proto/add (get-repo this) format triple-stream))
  ([this graph base-uri format triple-stream] (proto/add (get-repo this) format triple-stream)))

;;ITripleWritable
(def default-triple-writeable-impl
  {:add-statement add-statement-impl
   :add add-impl})
