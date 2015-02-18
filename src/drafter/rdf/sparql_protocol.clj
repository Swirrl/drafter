(ns drafter.rdf.sparql-protocol
  (:require [ring.util.io :as rio]
            [clojure.string :as str]
            [grafter.rdf.repository :as repo]
            [drafter.operations :refer :all]
            [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [clojure.tools.logging :as log]
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
            BindingSet QueryLanguage BooleanQuery GraphQuery]
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

(defn new-result-writer [writer-class ostream]
  (.newInstance
   (.getConstructor writer-class (into-array [java.io.OutputStream]))
   (into-array Object [ostream])))

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
               bs (do (doto (MapBindingSet.)
                        (.addBinding "s" (get draft->live s s))
                        (.addBinding "p" (get draft->live p p))
                        (.addBinding "o" (get draft->live o o))))]
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

(defn result-streamer [result-writer-class result-rewriter pquery response-mime-type result-notify-fn]
  "Returns a function that handles the errors and closes the SPARQL
  results stream when it's done.

  If an error is thrown the stream will be closed and an exception
  logged."
  (fn [ostream]
    (try
      (let [writer (let [w (new-result-writer result-writer-class ostream)]
                     (if result-rewriter
                       (result-rewriter w)
                       w))]
        (cond
         (instance? BooleanQuery pquery)
         (let [notifying-handler (notifying-query-result-handler result-notify-fn writer)
               result (.evaluate pquery)]
           (doto notifying-handler
             (.handleBoolean result)))

         (and (instance? QueryResultWriter writer)
              (instance? GraphQuery pquery))
         (do
           ;; Allow CSV and other tabular writers to work with graph
           ;; queries.
           (log/debug "pquery is " pquery " writer is " writer)
           (.evaluate pquery (notifying-rdf-handler result-notify-fn (result-handler-wrapper writer))))

         :else
         (do
           ;; Can be either a TupleQuery with QueryResultWriter or a
           ;; GraphQuery with an RDFHandler.
           (log/debug "pquery (default) is " pquery " writer is " writer)
           (cond
            (instance? TupleQuery pquery) (.evaluate pquery (notifying-query-result-handler result-notify-fn writer))
            (instance? GraphQuery pquery) (.evaluate pquery (notifying-rdf-handler result-notify-fn writer))))))

      (catch Exception ex
        ;; Note that if we error here it's now too late to return a
        ;; HTTP RESPONSE code error
        (log/error ex "Error streaming results"))

      (finally
        (.close ostream)))))

(defn get-sparql-response-content-type [mime-type]
  (if (= "text/html" mime-type)
    "text/plain"
    mime-type))

(def query-operations (atom {}))

(defn- stream-sparql-response [pquery response-mime-type result-rewriter]
  (if-let [result-writer-class (negotiate-content-writer pquery response-mime-type)]
    (let [{:keys [publish] :as query-operation} (create-operation query-operations)
          [write-fn input-stream] (connect-piped-output-stream (result-streamer result-writer-class result-rewriter
                                                                                pquery response-mime-type publish))
          _ (connect-operation query-operation write-fn)

          ;hard-code query timeouts for now - 60s for each result and 4 minutes for the entire operation
          ;TODO: make this configurable!
          query-operation-timeouts (create-timeouts 60000 240000)]

      (execute-operation query-operation query-operation-timeouts)

      {:status 200
       :headers {"Content-Type" (get-sparql-response-content-type response-mime-type)}
       ;; Designed to work with piped-input-stream this fn will be run
       ;; in another thread to stream the results to the client.
       :body input-stream})
    {:status 406
     :headers {"Content-Type" "text/plain"}
     :body (str "Unsupported media-type: " response-mime-type)}))

(defn restricted-dataset
  "Returns a restricted dataset or nil when given either a 0-arg
  function or a collection of graph uris."
  [graph-restrictions]
  (let [graph-restrictions (cond
                            (coll? graph-restrictions) graph-restrictions
                            (fn? graph-restrictions) (graph-restrictions)
                            :else nil)]

    (when graph-restrictions
      (repo/make-restricted-dataset :default-graph graph-restrictions
                                   :named-graphs graph-restrictions))))

(defn get-query-mime-preferences [query]
  (condp instance? query
    TupleQuery tuple-query-mime-preferences
    BooleanQuery boolean-query-mime-preferences
    GraphQuery graph-query-mime-preferences
    nil))

(defn negotiate-sparql-query-mime-type [query request]
  (let [mime-preferences (get-query-mime-preferences query)
        accept-handler (wrap-accept identity {:mime mime-preferences})
        mime (get-in (accept-handler request) [:accept :mime])]
    mime))

(defn process-sparql-query [db request & {:keys [query-creator-fn graph-restrictions
                                                 result-rewriter]
                                          :or {query-creator-fn repo/prepare-query}}]

  (let [restriction (restricted-dataset graph-restrictions)
        {:keys [headers params]} request
        query-str (:query params)
        pquery (doto (query-creator-fn db query-str)
                 (.setDataset restriction))
        media-type (negotiate-sparql-query-mime-type pquery request)]

    (log/info (str "Running query\n" query-str "\nwith graph restrictions: " graph-restrictions))
    (stream-sparql-response pquery media-type result-rewriter)))

(defn wrap-sparql-errors [handler]
  (fn [request]
    (try
      (handler request)
      (catch MalformedQueryException ex
        (let [error-message (.getMessage ex)]
          (log/info "Malformed query: " error-message)
          {:status 400 :headers {"Content-Type" "text/plain"} :body error-message})))))

(defn sparql-end-point
  "Builds a SPARQL end point from a mount-path, a sesame repository and
  an optional restriction function which returns a list of graph uris
  to restrict both the union and named-graph queries too."

  ([mount-path repo] (sparql-end-point mount-path repo nil))
  ([mount-path repo restrictions]
     ;; TODO make restriction-fn just the set of graphs to restrict to (or nil)
   (wrap-sparql-errors
    (routes
     (GET mount-path request
          (process-sparql-query repo request
                                :graph-restrictions restrictions))

     (POST mount-path request
           (process-sparql-query repo request
                                 :graph-restrictions restrictions))))))

(comment

  (take 10 (repo/query drafter.handler/repo "SELECT * WHERE { ?s ?p ?o }" :default-graph ["http://publishmydata.com/graphs/drafter/draft/ccb30b57-2b67-4e7b-a8a8-1a00aa2eeaf1"] :union-graph ["http://publishmydata.com/graphs/drafter/draft/ccb30b57-2b67-4e7b-a8a8-1a00aa2eeaf1"]))



  (take 10 (repo/evaluate (repo/prepare-query drafter.handler/repo "SELECT * WHERE { ?s ?p ?o }")))

  ;; Parsing a Query
  (org.openrdf.query.parser.QueryParserUtil/parseQuery QueryLanguage/SPARQL "SELECT * WHERE { ?s ?p ?o }" nil)

  (.getBindingNames (.getTupleExpr (org.openrdf.query.parser.QueryParserUtil/parseQuery QueryLanguage/SPARQL "SELECT * WHERE { ?s ?p ?o OPTIONAL { ?g ?z ?a }  }" nil)))

  )
