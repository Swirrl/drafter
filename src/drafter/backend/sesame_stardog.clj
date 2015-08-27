(ns drafter.backend.sesame-stardog
  (:require [environ.core :refer [env]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [drafter.util :as util]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame.common :refer :all]
            [drafter.backend.sesame.common.protocols :refer :all]
            [drafter.backend.sesame.common.sparql-execution :refer [create-execute-update-fn]]
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

;;default sesame implementation execute UPDATE queries in a transaction which the remote SPARQL
;;client does not like
(def ^:private execute-update-fn
  (create-execute-update-fn ->sesame-repo (fn [conn pquery] (repo/evaluate pquery))))

(defn- move-like-tbl-wants-super-slow-on-stardog-though
  "Move's how TBL intended.  Issues a SPARQL MOVE query.
  Note this is super slow on stardog 3.1."
  [source destination]
  ;; Move's how TBL intended...
  (str "MOVE SILENT <" source "> TO <" destination ">"))

(defn- delete-insert-move
  "Move source graph to destination.  Semantically the same as MOVE but done
  with DELETE/INSERT's.

  Massively quicker  on stardog than a MOVE."
  [source destination]
  (str
   ;; first clear the destination, then...
   "DELETE {"
   "  GRAPH <" destination "> {?s ?p ?o}"
   "} WHERE {"
   "  GRAPH <" destination "> {?s ?p ?o} "
   "};"
   ;; copy the source to the destination, and...
   "INSERT {"
   "  GRAPH <" destination "> {?s ?p ?o}"
   "} WHERE { "
   "  GRAPH <" source "> {?s ?p ?o}"
   "};"
   ;; remove the source (draft) graph
   "DELETE {"
   "  GRAPH <" source "> {?s ?p ?o}"
   "} WHERE {"
   " GRAPH <" source "> {?s ?p ?o}"
   "}"))

(defn graph-non-empty-query [graph-uri]
  (str
   "ASK WHERE {
    SELECT * WHERE {
      GRAPH <" graph-uri "> { ?s ?p ?o }
    } LIMIT 1
  }"))

(defn graph-non-empty?
  "Returns true if the graph contains any statements."
  [repo graph-uri]
  (repo/query repo (graph-non-empty-query graph-uri)))

(defn graph-empty?
  "Returns true if there are no statements in the associated graph."
  [repo graph-uri]
  (not (graph-non-empty? repo graph-uri)))

(defn should-delete-live-graph-from-state-after-draft-migrate?
  "When migrating a draft graph to live, the associated 'is managed
  graph' statement should be removed from the state if graph if:
  1. The migrate operation is a delete (i.e. the draft graph is empty)
  2. The migrated graph is the only draft associated with the live
  graph."
  [repo draft-graph-uri live-graph-uri]
  (and
   (graph-empty? repo draft-graph-uri)
   (not (mgmt/has-more-than-one-draft? repo live-graph-uri))))

;;Repository -> String -> { queries: [String], live-graph-uri: String }
(defn- migrate-live-queries [db draft-graph-uri]
  (if-let [live-graph-uri (mgmt/lookup-live-graph db draft-graph-uri)]
    (let [move-query (delete-insert-move draft-graph-uri live-graph-uri)
          delete-state-query (mgmt/delete-draft-state-query draft-graph-uri)
          live-public-query (mgmt/set-isPublic-query live-graph-uri true)
          queries [move-query delete-state-query live-public-query]
          queries (if (should-delete-live-graph-from-state-after-draft-migrate? db draft-graph-uri live-graph-uri)
                    (conj queries (mgmt/delete-live-graph-from-state-query live-graph-uri))
                    queries)]
      {:queries queries
       :live-graph-uri live-graph-uri})))

(defn- migrate-graphs-to-live-impl! [backend graphs]
  "Migrates a collection of draft graphs to live through a single
  compound SPARQL update statement. This overrides the default sesame
  implementation which uses a transaction to coordinate the
  updates. Explicit UPDATE statements do not take part in transactions
  on the remote sesame SPARQL client."
  (log/info "Starting make-live for graphs " graphs)
  (let [repo (->sesame-repo backend)
        graph-migrate-queries (mapcat #(:queries (migrate-live-queries repo %)) graphs)
        update-str (util/make-compound-sparql-query graph-migrate-queries)]
    (update! repo update-str))
  (log/info "Make-live for graph(s) " graphs " done"))

(defn- append-data-batch [backend graph-uri triple-batch]
  ;;NOTE: The remote sesame client throws an exception if an empty transaction is committed
  ;;so only create one if there is data in the batch
  (if-not (empty? triple-batch)
    (with-open [conn (repo/->connection (->sesame-repo backend))]
      (repo/with-transaction conn
        (add conn graph-uri triple-batch)))))

(extend SesameStardogBackend
  proto/ITripleReadable default-triple-readable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor {:execute-update execute-update-fn}
  DraftManagement (assoc default-draft-management-impl :append-data-batch! append-data-batch :migrate-graphs-to-live! migrate-graphs-to-live-impl!)
  ApiOperations default-api-operations-impl
  Stoppable default-stoppable-impl
  ToRepository {:->sesame-repo :repo}

  ;TODO: remove? required by the default delete-graph-job implementation which deletes
  ;;in batches. This could be a simple DROP on Stardog
  SesameBatchOperations default-sesame-batch-operations-impl)

(defn get-stardog-backend [env-map]
  (let [repo (get-stardog-repo env-map)]
    (->SesameStardogBackend repo)))
