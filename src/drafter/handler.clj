(ns drafter.handler
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [compojure.core :refer [defroutes context]]
            [compojure.route :as route]
            [drafter.operations :as ops]
            [drafter.middleware :as middleware]
            [drafter.configuration :as conf]
            [drafter.util :refer [set-var-root!]]
            [drafter.common.json-encoders :as enc]
            [drafter.rdf.draft-management :refer [lookup-live-graph-uri]]
            [drafter.rdf.sparql-rewriting :refer [function-registry
                                                  pmdfunctions
                                                  register-function!]]
            [drafter.routes.drafts-api :refer [draft-api-routes
                                               graph-management-routes]]
            [drafter.routes.status :refer [status-routes]]
            [drafter.routes.dumps :refer [dumps-endpoint]]
            [drafter.routes.pages :refer [pages-routes]]
            [drafter.routes.sparql :refer [draft-sparql-routes
                                           live-sparql-routes
                                           raw-sparql-routes
                                           state-sparql-routes]]
            [drafter.routes.sparql-update :refer [live-update-endpoint-route
                                                  raw-update-endpoint-route
                                                  state-update-endpoint-route]]
            [drafter.write-scheduler :refer [start-writer!
                                             global-writes-lock
                                             stop-writer!]]
            [swirrl-server.async.jobs :refer [restart-id finished-jobs]]
            [environ.core :refer [env]]
            [grafter.rdf.repository :as repo]
            [noir.util.middleware :refer [app-handler]]
            [ring.middleware.verbs :refer [wrap-verbs]]
            [selmer.parser :as parser])

  ;; Note that though the classes and requires below aren't used in this namespace
  ;; they are needed by the log-config file which is loaded from here.
  (:import [org.apache.log4j ConsoleAppender DailyRollingFileAppender EnhancedPatternLayout PatternLayout SimpleLayout]
           [org.apache.log4j.helpers DateLayout]
           [java.util UUID]
           [java.nio.charset Charset]
           [org.openrdf.query.resultio BooleanQueryResultParserRegistry TupleQueryResultParserRegistry]
           [org.openrdf.rio RDFParserRegistry]
           [org.openrdf.rio.ntriples NTriplesParserFactory]
           [org.openrdf.query.resultio TupleQueryResultFormat TupleQueryResultParserFactory BooleanQueryResultParserFactory BooleanQueryResultFormat]
           [org.openrdf.query.resultio.sparqlxml SPARQLResultsXMLParserFactory SPARQLResultsXMLParser SPARQLBooleanXMLParser]
           [org.openrdf.query.resultio.text.csv SPARQLResultsCSVParserFactory])

  (:require [clj-logging-config.log4j :refer [set-loggers!]]))


(def default-indexes "spoc,pocs,ocsp,cspo,cpos,oscp")

(def default-repo-path "drafter-db")

;; Set these values later when we start the server
(def repo)
(def app)

(def ^{:doc "A future to control the single write thread that performs database writes."}
  writer-service)

(def stop-reaper (fn []))

(defn register-sparql-extension-functions
  "Register custom drafter SPARQL extension functions."
  []

  ;; This function converts draft graphs into live graph URI's and is
  ;; necessary for drafters query/result rewriting to work.
  (register-function! function-registry
                      (pmdfunctions "replace-live-graph")
                      (partial lookup-live-graph-uri repo)))

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

;get-stardog-repo :: String -> Repository
(defn get-stardog-repo [stardog-db-endpoint]
  (register-stardog-query-mime-types!)
  (repo/sparql-repo stardog-db-endpoint))

(def default-stardog-drafter-endpoint "http://localhost:5820/drafter/query")

(defn get-stardog-drafter-sparql-endpoint [env-map]
  (if-let [endpoint (:stardog-drafter-sparql-endpoint env-map)]
    endpoint
    (do
      (log/warn  "SPARQL endpoint for drafter database not configured - using default (" default-stardog-drafter-endpoint ")")
      default-stardog-drafter-endpoint)))

(defn initialise-repo! [repo-path indexes]
  (set-var-root! #'repo (let [endpoint (get-stardog-drafter-sparql-endpoint env)
                              repo (get-stardog-repo endpoint)]
                          (log/info "Initialised repo" repo-path)
                          repo))

  (register-sparql-extension-functions))

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(def add-route conj)
(def add-routes into)

(defn- log-endpoint-config [ep-name endpoint-type config]
  (log/trace (str (name endpoint-type) " endpoint for route '" (name ep-name) "': " config)))

(defn- endpoint-query-path [route-name]
  (str "/sparql/" (name route-name)))

(defn endpoint-update-path [route-name]
  (str (endpoint-query-path route-name) "/update"))

(defn- endpoint-route [route-name path-fn endpoint-fn route-type repo timeout-config]
  (let [path (path-fn route-name)
        timeouts (conf/get-endpoint-timeout route-name route-type timeout-config)]
    (log-endpoint-config route-name route-type timeouts)
    (endpoint-fn path repo timeouts)))

(defn- dumps-route [route-name query-fn timeout-config]
  (let [dump-path (str "/data/" (name route-name))
        query-timeouts (conf/get-endpoint-timeout route-name :query timeout-config)
        dump-fn #(query-fn %1 %2 query-timeouts)]
    (dumps-endpoint dump-path dump-fn repo)))

(defn create-sparql-endpoint-routes [route-name query-fn update-fn add-dumps? repo timeout-config]
  (let [query-route (endpoint-route route-name endpoint-query-path query-fn :query repo timeout-config)
        update-route (and update-fn (endpoint-route route-name endpoint-update-path update-fn :update repo timeout-config))
        dumps-route (if add-dumps? (dumps-route route-name query-fn timeout-config) nil)
        routes [query-route update-route]]
    (vec (remove nil? [query-route update-route dumps-route]))))

(defn specify-endpoint [query-fn update-fn has-dump?]
  {:query-fn query-fn :update-fn update-fn :has-dump has-dump?})

(def live-endpoint-spec (specify-endpoint live-sparql-routes live-update-endpoint-route true))
(def raw-endpoint-spec (specify-endpoint raw-sparql-routes raw-update-endpoint-route true))
(def draft-endpoint-spec (specify-endpoint draft-sparql-routes nil true))
(def state-endpoint-spec (specify-endpoint state-sparql-routes state-update-endpoint-route false))

(defn create-sparql-routes [endpoint-map repo]
  (let [timeout-conf (conf/get-timeout-config env (keys endpoint-map) ops/default-timeouts)
        ep-routes (fn [[ep-name {:keys [query-fn update-fn has-dump]}]]
                    (create-sparql-endpoint-routes ep-name query-fn update-fn has-dump repo timeout-conf))]
    (mapcat ep-routes endpoint-map)))

(defn get-sparql-routes [repo]
  (let [endpoints {:live live-endpoint-spec
                   :raw raw-endpoint-spec
                   :draft draft-endpoint-spec
                   :state state-endpoint-spec}]
    (create-sparql-routes endpoints repo)))

(defn initialise-app! [repo]
  (set-var-root! #'app (app-handler
                        ;; add your application routes here
                        (-> []
                            (add-route (pages-routes repo))
                            (add-route (draft-api-routes "/draft" repo))
                            (add-route (graph-management-routes "/graph" repo))
                            (add-routes (get-sparql-routes repo))
                            (add-route (context "/status" []
                                                (status-routes global-writes-lock finished-jobs restart-id)))

                            (add-route app-routes))

                        ;; add custom middleware here
                        :middleware [wrap-verbs
                                     middleware/template-error-page
                                     middleware/log-request]
                        ;; add access rules here
                        :access-rules []
                        ;; serialize/deserialize the following data formats
                        ;; available formats:
                        ;; :json :json-kw :yaml :yaml-kw :edn :yaml-in-html
                        :formats [:json-kw :edn])))

(defn initialise-write-service! []
  (set-var-root! #'writer-service  (start-writer!)))

(defn initialise-services! [repo-path indexes]
  (enc/register-custom-encoders!)
  (initialise-repo! repo-path indexes)

  (initialise-write-service!)
  (initialise-app! repo)
  (set-var-root! #'stop-reaper (ops/start-reaper 2000)))

(defn- load-logging-configuration [config-file]
  (-> config-file slurp read-string))

(defn configure-logging! [config-file]
  (binding [*ns* (find-ns 'drafter.handler)]
    (let [default-config (load-logging-configuration (io/resource "log-config.edn"))
          config-file (when (.exists config-file)
                        (load-logging-configuration config-file))]

      (let [chosen-config (or config-file default-config)]
        (eval chosen-config)
        (log/debug "Loaded logging config" chosen-config)))))

(defn init
  "init will be called once when app is deployed as a servlet on an
  app server such as Tomcat put any initialization code here"
  []

  (configure-logging! (io/file (get env :log-config-file "log-config.edn")))

  (when (env :dev)
    (parser/cache-off!))

  (log/info "Initialising repository")
  (initialise-services! (get env :drafter-repo-path default-repo-path)
                        ;; http://sw.deri.org/2005/02/dexa/yars.pdf - see table on p5 for full coverage of indexes.
                        ;; (but we have to specify 4 char strings, so in some cases last chars don't matter
                        (get env :drafter-indexes default-indexes))

  (log/info "drafter started successfully"))

(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (log/info "drafter is shutting down.  Please wait (this can take a minute)...")
  (repo/shutdown repo)
  (stop-writer! writer-service)
  (stop-reaper)
  (log/info "drafter has shut down."))

(defn reindex
  "Reindex the database according to the DRAFTER_INDEXES set at
  DRAFTER_REPO_PATH in the environment.  If no environment variables
  are set for these values the defaults are used."
  []
  (let [indexes (get env :drafter-indexes default-indexes)
        repo-path (get env :drafter-repo-path default-repo-path)]
    (log/info "Reindexing database at" repo-path " with indexes" indexes)
    (repo/repo (repo/native-store repo-path indexes))
    (log/info "Reindexing finished")))
