(ns drafter.backend.sesame-stardog
  (:require [environ.core :refer [env]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [drafter.util :as util]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame-common :refer :all]
            [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.draft-management.jobs :as jobs]
            [grafter.rdf.protocols :refer [update!]]
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
(defn- get-repo [backend] (:repo backend))

;;default sesame implementation execute UPDATE queries in a transaction which the remote SPARQL
;;client does not like
(def ^:private execute-update-fn
  (create-execute-update-fn get-repo (fn [conn pquery] (repo/evaluate pquery))))

(defn- migrate-graphs-to-live-impl! [backend graphs]
  (log/info "Starting make-live for graph" graphs)
  (let [repo (get-repo backend)
        graph-migrate-queries (mapcat #(:queries (mgmt/migrate-live-queries repo %)) graphs)
        update-str (util/make-compound-sparql-query graph-migrate-queries)]
    (update! repo update-str))
  (log/info "Make-live for graph(s) " graphs " done"))

(defn- batch-migrate-graphs-to-live-job
  "Migrates a collection of draft graphs to live through a single
  compound SPARQL update statement. This overrides the default sesame
  implementation which uses a transaction to coordinate the
  updates. Explicit UPDATE statements do not take part in transactions
  on the remote sesame SPARQL client."
  [backend graphs]
  (jobs/make-job :exclusive-write [job]
                 (migrate-graphs-to-live-impl! backend graphs)
                 (jobs/job-succeeded! job)))

(defn- append-data-batch [backend graph-uri triple-batch]
  ;;NOTE: The remote sesame client throws an exception if an empty transaction is committed
  ;;so only create one if there is data in the batch
  (if-not (empty? triple-batch)
    (with-open [conn (repo/->connection (get-repo backend))]
      (repo/with-transaction conn
        (add conn graph-uri triple-batch)))))

(extend SesameStardogBackend
  repo/ToConnection default-to-connection-impl
  proto/ITripleReadable default-triple-readable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor {:execute-update execute-update-fn}
  DraftManagement (assoc default-draft-management-impl :append-data-batch! append-data-batch :migrate-graphs-to-live! migrate-graphs-to-live-impl!)
  ApiOperations (assoc default-api-operations-impl :migrate-graphs-to-live-job batch-migrate-graphs-to-live-job)
  Stoppable default-stoppable-impl

  ;TODO: remove? required by the default delete-graph-job implementation which deletes
  ;;in batches. This could be a simple DROP on Stardog
  SesameBatchOperations default-sesame-batch-operations-impl)

(defn get-stardog-backend [env-map]
  (let [repo (get-stardog-repo env-map)]
    (->SesameStardogBackend repo)))
