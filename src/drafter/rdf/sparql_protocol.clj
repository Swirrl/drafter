(ns drafter.rdf.sparql-protocol
  (:require [ring.util.io :as rio]
            [clojure.string :as str]
            [grafter.rdf.sesame :as ses]
            [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [clojure.tools.logging :as log])
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
           [org.openrdf.query Dataset]
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

(def mime-type-preferences ["application/n-triples" :qs 1.0,
                            "application/n-quads" :qs 0.9,
                            "text/turtle" :qs 0.9,
                            "application/rdf+xml" :qs 0.9,
                            "text/n3" :qs 0.9,
                            "application/sparql-results+xml" :qs 0.9,
                            "application/sparql-results+json" :qs 0.9,
                            "text/csv" :qs 0.8,
                            "application/trig" :qs 0.8,
                            "application/trix" :qs 0.8,
                            "application/x-binary-rdf" :qs 0.7,
                            "text/plain" :qs 0.7,
                            "text/html" :qs 0.7,
                            "text/tab-separated-values" :qs 0.7])

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

(defn result-streamer [result-writer-class result-rewriter pquery response-mime-type]
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
         (instance? BooleanTextWriter writer)
         (let [result (.evaluate pquery)]
            (doto (BooleanTextWriter. ostream)
              (.handleBoolean result)))

         (and (instance? QueryResultWriter writer)
              (instance? GraphQuery pquery))
         (do (log/debug "pquery is " pquery " writer is " writer)
             (.evaluate pquery (result-handler-wrapper writer)))

         :else
         (do
           (log/debug "pquery (default) is " pquery " writer is " writer)
           (.evaluate pquery writer))))

      (catch Exception ex
        ;; Note that if we error here it's now too late to return a
        ;; HTTP RESPONSE code error
        (log/error ex "Error streaming results"))

      (finally
        (.close ostream)))))

(defn- stream-sparql-response [pquery response-mime-type result-rewriter]
  (if-let [result-writer-class (negotiate-content-writer pquery response-mime-type)]
    {:status 200
     :headers {"Content-Type" response-mime-type}
     ;; Designed to work with piped-input-stream this fn will be run
     ;; in another thread to stream the results to the client.
     :body (rio/piped-input-stream (result-streamer result-writer-class result-rewriter
                                                    pquery response-mime-type))}
    {:status 406
     :headers {"Content-Type" "text/plain"}
     :body (str "Unsupported media-type: " response-mime-type)}))

(defn parse-accept
  "Stupid accept header parsing"
  [headers]
  (let [accept-str (get headers "accept")
        fst (-> accept-str
             (str/split #",")
             first)]
    (or fst accept-str)))

(defn restricted-dataset
  "Returns a restricted dataset or nil when given either a 0-arg
  function or a collection of graph uris."
  [graph-restrictions]
  (let [graph-restrictions (cond
                            (coll? graph-restrictions) graph-restrictions
                            (fn? graph-restrictions) (graph-restrictions)
                            :else nil)]

    (when graph-restrictions
      (ses/make-restricted-dataset :default-graph graph-restrictions
                                   :named-graphs graph-restrictions))))

(defn process-sparql-query [db request & {:keys [query-creator-fn graph-restrictions
                                                 result-rewriter]
                                          :or {query-creator-fn ses/prepare-query}}]

  (let [restriction (restricted-dataset graph-restrictions)
        {:keys [headers params]} request
        query-str (:query params)
        pquery (doto (query-creator-fn db query-str)
                 (.setDataset restriction))
        media-type (parse-accept headers)]

    (log/info (str "Running query\n" query-str "\nwith graph restrictions: " graph-restrictions))
    (stream-sparql-response pquery media-type result-rewriter)))

(defn sparql-end-point
  "Builds a SPARQL end point from a mount-path, a sesame repository and
  an optional restriction function which returns a list of graph uris
  to restrict both the union and named-graph queries too."

  ([mount-path repo] (sparql-end-point mount-path repo nil))

  ([mount-path repo restrictions]
     ;; TODO make restriction-fn just the set of graphs to restrict to (or nil)
     (routes
      (ring.middleware.accept/wrap-accept
       (GET mount-path request
            (process-sparql-query repo request
                                  :graph-restrictions restrictions))
       {:mime mime-type-preferences})

      (ring.middleware.accept/wrap-accept
       (POST mount-path request
             (process-sparql-query repo request
                                   :graph-restrictions restrictions))
       {:mime mime-type-preferences}))))

(comment

  (take 10 (ses/query drafter.handler/repo "SELECT * WHERE { ?s ?p ?o }" :default-graph ["http://publishmydata.com/graphs/drafter/draft/ccb30b57-2b67-4e7b-a8a8-1a00aa2eeaf1"] :union-graph ["http://publishmydata.com/graphs/drafter/draft/ccb30b57-2b67-4e7b-a8a8-1a00aa2eeaf1"]))



  (take 10 (ses/evaluate (ses/prepare-query drafter.handler/repo "SELECT * WHERE { ?s ?p ?o }")))

  ;; Parsing a Query
  (org.openrdf.query.parser.QueryParserUtil/parseQuery QueryLanguage/SPARQL "SELECT * WHERE { ?s ?p ?o }" nil)

  (.getBindingNames (.getTupleExpr (org.openrdf.query.parser.QueryParserUtil/parseQuery QueryLanguage/SPARQL "SELECT * WHERE { ?s ?p ?o OPTIONAL { ?g ?z ?a }  }" nil)))

  )
