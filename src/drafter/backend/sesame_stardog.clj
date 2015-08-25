(ns drafter.backend.sesame-stardog
  (:require [environ.core :refer [env]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame-common :refer [default-sparql-update-impl default-stoppable-impl default-sparql-query-impl
                                                   default-to-connection-impl default-sparqlable-impl default-triple-readable-impl
                                                   default-isparql-updatable-impl default-query-rewritable-impl default-api-operations-impl]]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :as proto])
  (:import [java.nio.charset Charset]
           [org.openrdf.query.resultio BooleanQueryResultParserRegistry TupleQueryResultParserRegistry]
           [org.openrdf.rio RDFParserRegistry]
           [org.openrdf.rio.ntriples NTriplesParserFactory]
           [org.openrdf.query.resultio TupleQueryResultFormat TupleQueryResultParserFactory BooleanQueryResultParserFactory BooleanQueryResultFormat]
           [org.openrdf.query.resultio.sparqlxml SPARQLResultsXMLParserFactory SPARQLResultsXMLParser SPARQLBooleanXMLParser]
           [org.openrdf.query.resultio.text.csv SPARQLResultsCSVParserFactory]
           [drafter.rdf DrafterSPARQLRepository]))

(defn- set-supported-file-formats! [registry formats]
  ;clear registry
  (doseq [pf (vec (.getAll registry))]
    (.remove registry pf))

  ;re-populate
  (doseq [f formats] (.add registry f)))

(def utf8-charset (Charset/forName "UTF-8"))

(defn get-sparql-boolean-xml-parser-factory []
  (let [result-format (BooleanQueryResultFormat. "SPARQL/XML" ["application/sparql-results+xml"] utf8-charset ["srx" "xml"])]
    (reify BooleanQueryResultParserFactory
      (getBooleanQueryResultFormat [this] result-format)
      (getParser [this] (SPARQLBooleanXMLParser.)))))

(defn get-tuple-result-xml-parser-factory []
  (let [result-format (TupleQueryResultFormat. "SPARQL/XML" ["application/sparql-results+xml"] utf8-charset ["srx" "xml"])]
    (reify TupleQueryResultParserFactory
      (getTupleQueryResultFormat [this] result-format)
      (getParser [this] (SPARQLResultsXMLParser.)))))

(defn register-stardog-query-mime-types!
  "Stardog's SPARQL endpoint does not support content negotiation and
  appears to pick the first accepted MIME type sent by the client. If
  this MIME type is not supported then an error response is returned,
  even if other MIME types accepted by the client are
  supported. Sesame maintains a global registry of supported formats
  for each type of query (tuple, graph, boolean) along with their
  associated MIME types. These are used to populate the accept headers
  in the query request. This function clears the format registries and
  then re-populates them only with ones stardog supports.

  WARNING: This may have an impact on the functionality of other
  sesame functionality, although drafter should only need it when
  using the SPARQL repository."
  []
  (set-supported-file-formats! (TupleQueryResultParserRegistry/getInstance) [(get-tuple-result-xml-parser-factory)])
  (set-supported-file-formats! (BooleanQueryResultParserRegistry/getInstance) [(get-sparql-boolean-xml-parser-factory)])
  (set-supported-file-formats! (RDFParserRegistry/getInstance) [(NTriplesParserFactory.)]))

(defn get-required-environment-variable [var-key env-map]
  (if-let [ev (var-key env-map)]
    ev
    (let [var-name (str/upper-case (str/replace (name var-key) \- \_))
          message (str "Missing required key "
                       var-key
                       " in environment map. Ensure you export an environment variable "
                       var-name
                       " or define "
                       var-key
                       " in the relevant profile in profiles.clj")]
      (do
        (log/error message)
        (throw (RuntimeException. message))))))

;get-stardog-repo :: {String String} -> Repository
(defn- get-stardog-repo [env-map]
  (let [query-endpoint (get-required-environment-variable :sparql-query-endpoint env-map)
        update-endpoint (get-required-environment-variable :sparql-update-endpoint env-map)]
    (register-stardog-query-mime-types!)
    (let [repo (DrafterSPARQLRepository. query-endpoint update-endpoint)]
      (.initialize repo)
      (log/info "Initialised repo at QUERY=" query-endpoint ", UPDATE=" update-endpoint)
      repo)))

(defrecord SesameStardogBackend [repo])

(extend SesameStardogBackend
  repo/ToConnection default-to-connection-impl
  proto/ITripleReadable default-triple-readable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor default-sparql-update-impl
  ApiOperations default-api-operations-impl
  Stoppable default-stoppable-impl)

(defn get-stardog-backend [env-map]
  (let [repo (get-stardog-repo env-map)]
    (->SesameStardogBackend repo)))
