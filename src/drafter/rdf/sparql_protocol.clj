(ns drafter.rdf.sparql-protocol
  (:require [clojure.tools.logging :as log]
            [compojure.core :refer [GET POST routes]]
            [drafter.util :refer [construct-dynamic*]]
            [drafter.operations :refer :all]
            [drafter.rdf.rewriting.arq :refer [->sparql-string sparql-string->arq-query]]
            [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [clojure.tools.logging :as log]
            [ring.middleware.accept :refer [wrap-accept]]
            [grafter.rdf.repository :as repo]
            [ring.middleware.accept :refer [wrap-accept]])
  (:import [org.openrdf.query GraphQuery]
           [org.openrdf.model Statement Value Resource Literal URI BNode ValueFactory]
           [org.openrdf.model.impl CalendarLiteralImpl ValueFactoryImpl URIImpl
            BooleanLiteralImpl LiteralImpl IntegerLiteralImpl NumericLiteralImpl
            StatementImpl BNodeImpl ContextStatementImpl]
           [org.openrdf.repository Repository RepositoryConnection]
           [org.openrdf.repository.sail SailRepository]
           [org.openrdf.sail.memory MemoryStore]
           [org.openrdf.rio Rio RDFWriter RDFHandler]
           [org.openrdf.sail.nativerdf NativeStore]
           [org.openrdf.query TupleQuery TupleQueryResult
            TupleQueryResultHandler BooleanQueryResultHandler
            BindingSet QueryLanguage BooleanQuery GraphQuery Update]
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
           [org.openrdf.query Dataset MalformedQueryException]
           [org.openrdf.query.resultio QueryResultWriter]
           [org.openrdf.query.impl DatasetImpl MapBindingSet]
           [javax.xml.datatype XMLGregorianCalendar DatatypeFactory]
           [org.apache.jena.query QueryParseException]
           [java.util GregorianCalendar Date]
           [org.openrdf.rio RDFFormat]))

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

(defn mime-pref [mime q] [mime :as mime :qs q])

(defn mime-table [& preferences]
  (apply vector (apply concat preferences)))

(def tuple-query-mime-preferences
  (mime-table (mime-pref "application/sparql-results+json" 0.9)
              (mime-pref "application/sparql-results+xml" 0.9)
              (mime-pref "application/x-binary-rdf" 0.7)
              (mime-pref "text/csv" 1.0)
              (mime-pref "text/tab-separated-values" 0.8)
              (mime-pref "text/plain" 1.0)
              (mime-pref "text/html" 1.0)))

(def boolean-query-mime-preferences
  (mime-table (mime-pref "application/sparql-results+xml" 1.0)
              (mime-pref "application/sparql-results+json" 1.0)
              (mime-pref "application/x-binary-rdf" 0.7)
              (mime-pref "text/plain" 0.9)
              (mime-pref "text/html" 0.8)))

(def graph-query-mime-preferences
  (mime-table (mime-pref "application/n-triples" 1.0)
              (mime-pref "application/n-quads" 0.9)
              (mime-pref "text/n3" 0.9)
              (mime-pref "application/trig" 0.8)
              (mime-pref "application/trix" 0.8)
              (mime-pref "text/turtle" 0.9)
              (mime-pref "text/html" 0.7)
              (mime-pref "application/rdf+xml" 0.9)
              (mime-pref "text/csv" 0.8)
              (mime-pref "text/tab-separated-values" 0.7)))

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

;result-streamer :: (OutputStream -> NotifierFn -> ()) -> NotifierFn -> (OutputStream -> ())
(defn result-streamer [exec-fn result-notify-fn]
  "Returns a function that handles the errors and closes the SPARQL
  results stream when it's done.

  If an error is thrown the stream will be closed and an exception
  logged."
  (fn [ostream]
    (try
      (exec-fn ostream result-notify-fn)

      (catch Exception ex
        ;; Note that if we error here it's now too late to return a
        ;; HTTP RESPONSE code error
        (log/error ex "Error streaming results"))

      (finally
        (.close ostream)))))

(defn get-sparql-response-content-type [mime-type]
  (case mime-type
    ;; if they ask for html they're probably a browser so serve it as
    ;; text/plain
    "text/html" "text/plain; charset=utf-8"
    ;; force a charset of UTF-8 in this case... NOTE this should
    ;; really consult the Accept-Charset header
    "text/plain" "text/plain; charset=utf-8"
    mime-type))

(defn- unsupported-media-type-response [media-type]
  {:status 406
   :headers {"Content-Type" "text/plain; charset=utf-8"}
   :body (str "Unsupported media-type: " media-type)})

(defn- stream-sparql-response [exec-fn query-timeouts]
  (let [{:keys [publish] :as query-operation} (create-operation)
        streamer (result-streamer exec-fn publish)
        [write-fn input-stream] (connect-piped-output-stream streamer)]

      (execute-operation query-operation write-fn query-timeouts)
      input-stream))

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

(defn get-query-mime-preferences [query-type]
  (case query-type
    :select tuple-query-mime-preferences
    :ask boolean-query-mime-preferences
    :construct graph-query-mime-preferences
    nil))

(defn negotiate-sparql-query-mime-type [query-type request]
  (let [mime-preferences (get-query-mime-preferences query-type)
        accept-handler (wrap-accept identity {:mime mime-preferences})
        mime (get-in (accept-handler request) [:accept :mime])]
    mime))

;class->writer-fn :: Class[T] -> (OutputStream -> T)
(defn class->writer-fn [writer-class]
  (fn [output-stream]
    (construct-dynamic* writer-class output-stream)))

(defn validate-query [query-str]
  "Validates a query by parsing it using ARQ. If the query is invalid
  a QueryParseException is thrown."
  (sparql-string->arq-query query-str)
  query-str)

(defprotocol SparqlExecutor
  (prepare-query [this sparql-string restrictions])
  (get-query-type [this prepared-query])
  (negotiate-result-writer [this prepared-query media-type])
  (create-query-executor [this writer pquery]))

(defprotocol SparqlUpdateExecutor
  (execute-update [this update-query restrictions]))

(defrecord SesameSparqlExecutor [repo]
  SparqlExecutor
  (prepare-query [_ sparql-string graph-restrictions]
    (let [validated-query-string (validate-query sparql-string)
          dataset (restricted-dataset graph-restrictions)
          pquery (repo/prepare-query repo validated-query-string)]
      (.setDataset pquery dataset)
      pquery))

  (get-query-type [_ pquery]
    (condp instance? pquery
      TupleQuery :select
      BooleanQuery :ask
      GraphQuery :construct
      Update :update
      nil))

  (negotiate-result-writer [_ pquery media-type]
    (if-let [writer-class (negotiate-content-writer pquery media-type)]
      (class->writer-fn writer-class)))

  (create-query-executor [_ writer-fn pquery]
    (get-exec-query pquery writer-fn))

  SparqlUpdateExecutor
  (execute-update [_ update-query restrictions]
    (with-open [conn (repo/->connection repo)]
      (let [dataset (restricted-dataset restrictions)
            pquery (repo/prepare-update conn update-query dataset)]
        (repo/with-transaction conn
          (repo/evaluate pquery))))))

(defrecord RewritingSesameSparqlExecutor [inner query-rewriter result-rewriter]
  SparqlExecutor
  (prepare-query [_ sparql-string restrictions]
    (prepare-query inner (query-rewriter sparql-string) restrictions))

  (get-query-type [_ pquery]
    (get-query-type inner pquery))

  (negotiate-result-writer [_ pquery media-type]
    (if-let [inner-writer (negotiate-result-writer inner pquery media-type)]
      #(result-rewriter pquery (inner-writer %))))

  (create-query-executor [_ writer-fn pquery]
    (create-query-executor inner writer-fn pquery)))

(defn process-sparql-query [executor request & {:keys [graph-restrictions query-timeouts]
                                                :or {query-timeouts default-timeouts}}]
  (let [query-str (get-in request [:params :query])
        pquery (prepare-query executor query-str graph-restrictions)
        query-type (get-query-type executor pquery)
        media-type (negotiate-sparql-query-mime-type query-type request)]

    (log/info (str "Running query\n" query-str "\nwith graph restrictions"))

    (if-let [writer (negotiate-result-writer executor pquery media-type)]
      (let [exec-fn (create-query-executor executor writer pquery)
            body (stream-sparql-response exec-fn query-timeouts)
            response-content-type (get-sparql-response-content-type media-type)]
        {:status 200
         :headers {"Content-Type" response-content-type}
         ;; Designed to work with piped-input-stream this fn will be run
         ;; in another thread to stream the results to the client.
         :body body})
      
      (unsupported-media-type-response media-type))))

(defn wrap-sparql-errors [handler]
  (fn [request]
    (try
      (handler request)
      (catch QueryParseException ex
        (let [error-message (.getMessage ex)]
          (log/info "Malformed query: " error-message)
          {:status 400 :headers {"Content-Type" "text/plain; charset=utf-8"} :body error-message})))))

(defn sparql-end-point
  "Builds a SPARQL end point from a mount-path, a sesame repository and
  an optional restriction function which returns a list of graph uris
  to restrict both the union and named-graph queries too."

  ([mount-path repo] (sparql-end-point mount-path repo nil))
  ([mount-path repo restrictions] (sparql-end-point mount-path repo restrictions nil))
  ([mount-path repo restrictions timeouts]
     ;; TODO make restriction-fn just the set of graphs to restrict to (or nil)
   (wrap-sparql-errors
    (routes
     (GET mount-path request
          (process-sparql-query (->SesameSparqlExecutor repo) request
                                :graph-restrictions restrictions
                                :query-timeouts timeouts))

     (POST mount-path request
           (process-sparql-query (->SesameSparqlExecutor repo) request
                                 :graph-restrictions restrictions
                                 :query-timeouts timeouts))))))

(comment

  (take 10 (repo/query drafter.handler/repo "SELECT * WHERE { ?s ?p ?o }" :default-graph ["http://publishmydata.com/graphs/drafter/draft/ccb30b57-2b67-4e7b-a8a8-1a00aa2eeaf1"] :union-graph ["http://publishmydata.com/graphs/drafter/draft/ccb30b57-2b67-4e7b-a8a8-1a00aa2eeaf1"]))



  (take 10 (repo/evaluate (repo/prepare-query drafter.handler/repo "SELECT * WHERE { ?s ?p ?o }")))

  ;; Parsing a Query
  (org.openrdf.query.parser.QueryParserUtil/parseQuery QueryLanguage/SPARQL "SELECT * WHERE { ?s ?p ?o }" nil)

  (.getBindingNames (.getTupleExpr (org.openrdf.query.parser.QueryParserUtil/parseQuery QueryLanguage/SPARQL "SELECT * WHERE { ?s ?p ?o OPTIONAL { ?g ?z ?a }  }" nil)))

  )
