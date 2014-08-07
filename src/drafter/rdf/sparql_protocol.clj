(ns drafter.rdf.sparql-protocol
  (:require [ring.util.io :as rio]
            [clojure.string :as str]
            [grafter.rdf.sesame :as ses]
            [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [taoensso.timbre :as timbre])
  (:import [org.openrdf.model Statement Value Resource Literal URI BNode ValueFactory]
           [org.openrdf.model.impl CalendarLiteralImpl ValueFactoryImpl URIImpl
            BooleanLiteralImpl LiteralImpl IntegerLiteralImpl NumericLiteralImpl
            StatementImpl BNodeImpl ContextStatementImpl]
           [org.openrdf.repository Repository RepositoryConnection]
           [org.openrdf.query.resultio TupleQueryResultFormat]
           [org.openrdf.repository.sail SailRepository]
           [org.openrdf.sail.memory MemoryStore]
           [org.openrdf.rio Rio RDFWriter RDFHandler]
           [org.openrdf.sail.nativerdf NativeStore]
           [org.openrdf.query TupleQuery TupleQueryResult TupleQueryResultHandler BooleanQueryResultHandler BindingSet QueryLanguage BooleanQuery GraphQuery]
           [org.openrdf.query.resultio.text BooleanTextWriter]
           [org.openrdf.query.resultio.sparqljson SPARQLResultsJSONWriter]
           [org.openrdf.query.resultio.sparqlxml SPARQLResultsXMLWriter SPARQLBooleanXMLWriter]
           [org.openrdf.rio.ntriples NTriplesWriter]
           [org.openrdf.rio.nquads NQuadsWriter]
           [org.openrdf.rio.n3 N3Writer]
           [org.openrdf.rio.n3 N3Writer]
           [org.openrdf.rio.trig TriGWriter]
           [org.openrdf.rio.trix TriXWriter]
           [org.openrdf.rio.turtle TurtleWriter]
           [org.openrdf.rio.rdfxml RDFXMLWriter]
           [org.openrdf.query.resultio.binary BinaryQueryResultWriter]
           [org.openrdf.query.resultio.text.csv SPARQLResultsCSVWriter]
           [org.openrdf.query.resultio.text.tsv SPARQLResultsTSVWriter]
           [org.openrdf.query.impl DatasetImpl]
           [javax.xml.datatype XMLGregorianCalendar DatatypeFactory]
           [java.util GregorianCalendar Date]
           [org.openrdf.rio RDFFormat]))

(defn negotiate-content-type [query-type format]
  (condp instance? query-type
    TupleQuery   (get { "application/sparql-results+json" :json
                        "application/sparql-results+xml" :xml
                        "application/x-binary-rdf" :binary
                        "text/csv" :csv
                        "text/tab-separated-values" :tsv
                        "text/html" :csv
                        } format)
    BooleanQuery (get { "application/sparql-results+xml" :xml
                        "application/sparql-results+json" :json
                        "application/x-binary-rdf" :binary
                        "text/plain" :txt
                        "text/html" :txt
                        } format)
    GraphQuery   (get {
                       "application/n-triples" :ntriples
                       "application/n-quads" :nquads
                       "text/n3" :n3
                       "application/trig" :trig
                       "application/trix" :trix
                       "text/turtle" :turtle
                       "text/html" :turtle
                       "application/rdf+xml" :rdfxml
                       } format)

    nil))

(defmulti sparql-results! (fn [preped-query output-stream format]
                            (negotiate-content-type preped-query format)))

(defmethod sparql-results! :json [pquery output-stream format]
  (.evaluate pquery (SPARQLResultsJSONWriter. output-stream)))

(defmethod sparql-results! :xml [pquery output-stream format]
  (.evaluate pquery (SPARQLResultsXMLWriter. output-stream)))

(defmethod sparql-results! :binary [pquery output-stream format]
  (.evaluate pquery (BinaryQueryResultWriter. output-stream)))

(defmethod sparql-results! :csv [pquery output-stream format]
  (.evaluate pquery (SPARQLResultsCSVWriter. output-stream)))

(defmethod sparql-results! :tsv [pquery output-stream format]
  (.evaluate pquery (SPARQLResultsTSVWriter. output-stream)))

(defmethod sparql-results! :txt [pquery output-stream format]
  (let [result (.evaluate pquery)]
    (doto (BooleanTextWriter. output-stream)
      (.handleBoolean result))
    result))

;; graph formats

(defmethod sparql-results! :ntriples [pquery output-stream format]
  (.evaluate pquery (NTriplesWriter. output-stream)))

(defmethod sparql-results! :nquads [pquery output-stream format]
  (.evaluate pquery (NQuadsWriter. output-stream)))

(defmethod sparql-results! :n3 [pquery output-stream format]
  (.evaluate pquery (N3Writer. output-stream)))

(defmethod sparql-results! :trig [pquery output-stream format]
  (.evaluate pquery (TriGWriter. output-stream)))

(defmethod sparql-results! :trix [pquery output-stream format]
  (.evaluate pquery (TriXWriter. output-stream)))

(defmethod sparql-results! :turtle [pquery output-stream format]
  (.evaluate pquery (TurtleWriter. output-stream)))

(defmethod sparql-results! :rdfxml [pquery output-stream format]
  (.evaluate pquery (RDFXMLWriter. output-stream)))

(defmethod sparql-results! :default [pquery output-stream format]
  (throw (ex-info
          (str "Unsupported SPARQL response format for format: "
               format " with query-type " (class pquery)
               ". You must set an appropriate Accept header.")
          {:type :unsupported-media-type})))

(defn- make-streaming-sparql-response [pquery response-mime-type]
  (if (negotiate-content-type pquery response-mime-type)
    {:status 200
     :headers {"Content-Type" response-mime-type}
     :body (rio/piped-input-stream (fn [ostream]
                                     (try
                                       (sparql-results! pquery ostream response-mime-type)
                                       (catch clojure.lang.ExceptionInfo ex
                                         (timbre/error ex "Error streaming results")))
                                     (.close ostream)))}
    {:status 406
     :headers {"Content-Type" "text/plain"}
     :body (str "Unsupported media-type: " response-mime-type)}))

(defn parse-accept
  "Stupid accept header parsing"
  [accept-str]
  (let [fst (-> accept-str
                (str/split #",")
                first)]
    (or fst accept-str)))

(defn process-sparql-query [db request restriction-fn]
  (let [graphs (restriction-fn db)
        restriction (when graphs (ses/make-restricted-dataset :default-graph graphs
                                                              :named-graphs graphs))

        {:keys [headers params]} request
        query-str (:query params)
        pquery (ses/prepare-query db query-str restriction)
        media-type (-> (headers "accept")
                       parse-accept)]

    (timbre/debug (str "Running query " query-str " with graph restriction: " (apply str (interpose "," graphs))))
    (make-streaming-sparql-response pquery media-type)))

(defn sparql-end-point
  "Builds a SPARQL end point from a mount-path a sesame repository and
  an optional restriction function which returns a list of graph uris
  to restrict both the union and named-graph queries too."

  ([mount-path repo] (sparql-end-point mount-path repo (constantly nil)))

  ([mount-path repo restriction-fn]
     (routes
      (GET mount-path request
           (process-sparql-query repo request restriction-fn))
      (POST mount-path request
            (process-sparql-query repo request restriction-fn)))))
