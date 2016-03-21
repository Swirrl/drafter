(ns drafter.backend.sesame.common.sparql-execution
  (:require [clojure.tools.logging :as log]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :refer [map->Quad]]
            [grafter.rdf :refer [context]]
            [grafter.rdf.io :refer [IStatement->sesame-statement]]
            [clojure.set :as set]
            [drafter.backend.protocols :as backend]
            [drafter.backend.sesame.common.protocols :refer :all]
            [drafter.write-scheduler :as scheduler]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.draftset :as ds]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.sesame :refer [read-statements]]
            [swirrl-server.async.jobs :refer [create-job create-child-job]]
            [drafter.rdf.rewriting.query-rewriting :refer [rewrite-sparql-string]]
            [drafter.rdf.rewriting.result-rewriting :refer [result-handler-wrapper rewrite-query-results rewrite-statement]]
            [drafter.rdf.rewriting.arq :refer [->sparql-string sparql-string->arq-query]]
            [drafter.util :refer [construct-dynamic* map-values] :as util])
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
           [org.openrdf.query.impl MapBindingSet]
           [org.openrdf.model Resource]
           [org.openrdf.model.impl ContextStatementImpl URIImpl]))

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

;class->writer-fn :: Class[T] -> (OutputStream -> T)
(defn class->writer-fn [writer-class]
  (fn [output-stream]
    (construct-dynamic* writer-class output-stream)))

(def negotiate-graph-query-content-writer {
                       "application/n-triples" NTriplesWriter
                       "application/n-quads" NQuadsWriter
                       "text/x-nquads" NQuadsWriter
                       "text/n3" N3Writer
                       "application/trig" TriGWriter
                       "application/trix" TriXWriter
                       "text/turtle" TurtleWriter
                       "text/html" TurtleWriter
                       "application/rdf+xml" RDFXMLWriter
                       "text/csv" SPARQLResultsCSVWriter
                       "text/tab-separated-values" SPARQLResultsTSVWriter
                       })

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
         GraphQuery   negotiate-graph-query-content-writer
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

(defn- get-exec-query [writer-fn pquery]
  (fn [ostream notifier-fn]
    (let [writer (writer-fn ostream)]
      (cond
       (instance? BooleanQuery pquery)
       (exec-ask-query writer pquery notifier-fn)

       (instance? TupleQuery pquery)
       (exec-tuple-query writer pquery notifier-fn)

       :else
       (exec-graph-query writer pquery notifier-fn)))))

(defn create-query-executor [backend writer-fn prepared-query]
  (get-exec-query writer-fn prepared-query))

(defn validate-query [query-str]
  "Validates a query by parsing it using ARQ. If the query is invalid
  a QueryParseException is thrown."
  (sparql-string->arq-query query-str)
  query-str)

(defn prepare-query [backend sparql-string graph-restrictions]
    (let [repo (->sesame-repo backend)
          validated-query-string (validate-query sparql-string)
          dataset (restricted-dataset graph-restrictions)
          pquery (repo/prepare-query repo validated-query-string)]
      (.setDataset pquery dataset)
      pquery))

(defn- delete-quads-from-draftset [backend quad-batches draftset-ref live->draft {:keys [op job-started-at] :as state} job]
  (case op
    :delete
    (if-let [batch (first quad-batches)]
      (let [live-graph (context (first batch))]
        (if (mgmt/is-graph-managed? backend live-graph)
          (if-let [draft-graph-uri (get live->draft live-graph)]
            (do
              (mgmt/set-modifed-at-on-draft-graph! backend draft-graph-uri job-started-at)
              (with-open [conn (repo/->connection (->sesame-repo backend))]
                (let [rewritten-statements (map #(rewrite-statement live->draft %) batch)
                      sesame-statements (map IStatement->sesame-statement rewritten-statements)
                      graph-array (into-array Resource (map util/string->sesame-uri (vals live->draft)))]
                  (.remove conn sesame-statements graph-array)))
              (let [next-job (create-child-job
                              job
                              (partial delete-quads-from-draftset backend (rest quad-batches) draftset-ref live->draft state))]
                (scheduler/queue-job! next-job)))
            ;;NOTE: Do this immediately as we haven't done any real work yet
            (recur backend quad-batches draftset-ref live->draft (merge state {:op :copy-graph :live-graph live-graph}) job))
          ;;live graph does not exist so do not create a draft graph
          ;;NOTE: This is the same behaviour as deleting a live graph
          ;;which does not exist in live
          (recur backend (rest quad-batches) draftset-ref live->draft state job)))
      (let [draftset-info (dsmgmt/get-draftset-info backend draftset-ref)]
        (jobs/job-succeeded! job {:draftset draftset-info})))

    :copy-graph
    (let [{:keys [live-graph]} state
          draft-graph-uri (mgmt/create-draft-graph! backend live-graph {} (str (ds/->draftset-uri draftset-ref)))
          copy-batches (jobs/get-graph-clone-batches backend live-graph)
          copy-state {:op :copy-graph-batches
                      :graph live-graph
                      :draft-graph-uri draft-graph-uri
                      :batches copy-batches}]
      ;;NOTE: Do this immediately as no work has been done yet
      (recur backend quad-batches draftset-ref (assoc live->draft live-graph draft-graph-uri) (merge state copy-state) job))

    :copy-graph-batches
    (let [{:keys [graph batches draft-graph-uri]} state]
      (if-let [[offset limit] (first batches)]
        (do
          (jobs/copy-graph-batch! backend graph draft-graph-uri offset limit)
          (let [next-state (update-in state [:batches] rest)
                next-job (create-child-job
                          job
                          (partial delete-quads-from-draftset backend quad-batches draftset-ref live->draft (merge state next-state)))]
            (scheduler/queue-job! next-job)))
        ;;graph copy completed so continue deleting quads
        ;;NOTE: do this immediately since we haven't done any work on this iteration
        (recur backend quad-batches draftset-ref live->draft (merge state {:op :delete}) job)))))

(defn- batch-and-delete-quads-from-draftset [backend quads draftset-ref live->draft job]
  (let [quad-batches (util/batch-partition-by quads context jobs/batched-write-size)
        now (java.util.Date.)]
    (delete-quads-from-draftset backend quad-batches draftset-ref live->draft {:op :delete :job-started-at now} job)))

(defn- rdf-handler->spog-tuple-handler [rdf-handler]
  (reify TupleQueryResultHandler
    (handleSolution [this bindings]
      (let [subj (.getValue bindings "s")
            pred (.getValue bindings "p")
            obj (.getValue bindings "o")
            graph (.getValue bindings "g")
            stmt (ContextStatementImpl. subj pred obj graph)]
        (.handleStatement rdf-handler stmt)))

    (handleBoolean [this b])
    (handleLinks [this links])
    (startQueryResult [this binding-names]
      (.startRDF rdf-handler))
    (endQueryResult [this]
      (.endRDF rdf-handler))))

(defn- spog-tuple-query->graph-query [tuple-query]
  (reify GraphQuery
    (evaluate [this rdf-handler]
      (.evaluate tuple-query (rdf-handler->spog-tuple-handler rdf-handler)))))

(defn all-quads-query [backend graph-restrictions]
  (let [tuple-query (backend/prepare-query backend "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }" graph-restrictions)]
    (spog-tuple-query->graph-query tuple-query)))

(defn- get-prepared-query-type [pquery]
    (condp instance? pquery
      TupleQuery :select
      BooleanQuery :ask
      GraphQuery :construct
      Update :update
      nil))

(defn get-query-type [backend prepared-query]
  (get-prepared-query-type prepared-query))

(defn- negotiate-query-result-writer [pquery media-type]
    (if-let [writer-class (negotiate-content-writer pquery media-type)]
      (class->writer-fn writer-class)))

(defn negotiate-result-writer [backend prepared-query media-type]
  (negotiate-query-result-writer prepared-query media-type))

(defn execute-update-with [exec-prepared-update-fn backend update-query restrictions]
  (with-open [conn (->repo-connection backend)]
      (let [dataset (restricted-dataset restrictions)
            pquery (repo/prepare-update conn update-query dataset)]
        (exec-prepared-update-fn conn pquery))))

(defn- execute-prepared-update-in-transaction [conn prepared-query]
  (repo/with-transaction conn
    (repo/evaluate prepared-query)))

(defn execute-update [backend update-query restrictions]
  (execute-update-with execute-prepared-update-in-transaction backend update-query restrictions))

(defn- rewrite-value [mapping v]
  (get mapping v v))

(defrecord RewritingSesameSparqlExecutor [db live->draft]
  backend/SparqlExecutor
  (all-quads-query [this restrictions]
    (all-quads-query this restrictions))

  (prepare-query [_ sparql-string restrictions]
    (let [rewritten-query-string (rewrite-sparql-string live->draft sparql-string)
          prepared-query (backend/prepare-query db rewritten-query-string restrictions)]
      (rewrite-query-results prepared-query live->draft)))

  (get-query-type [_ pquery]
    (backend/get-query-type db pquery))

  (negotiate-result-writer [_ prepared-query media-type]
    (backend/negotiate-result-writer db prepared-query media-type))

  (create-query-executor [_ writer-fn pquery]
    (backend/create-query-executor db writer-fn pquery))

  backend/StatementDeletion
  (delete-quads-from-draftset-job [this serialised rdf-format draftset-ref]
    (jobs/make-job
     :batch-write [job]
     (let [quads (read-statements serialised rdf-format)]
       (batch-and-delete-quads-from-draftset db quads draftset-ref live->draft job))))

  (delete-triples-from-draftset-job [this serialised rdf-format draftset-ref graph]
    (jobs/make-job
     :batch-write [job]
     (let [triples (read-statements serialised rdf-format)
           quads (map #(util/make-quad-statement % graph) triples)]
       (batch-and-delete-quads-from-draftset db quads draftset-ref live->draft job)))))
