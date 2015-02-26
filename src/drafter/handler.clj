(ns drafter.handler
  (:require [compojure.core :refer [defroutes routes]]
            [ring.middleware.verbs :refer [wrap-verbs]]
            [drafter.routes.pages :refer [pages-routes]]
            [drafter.routes.sparql :refer [live-sparql-routes draft-sparql-routes
                                           state-sparql-routes raw-sparql-routes]]
            [drafter.routes.dumps :refer [dumps-endpoint]]
            [drafter.routes.drafts-api :refer [draft-api-routes graph-management-routes]]
            [drafter.middleware :as middleware]
            [drafter.rdf.sparql-rewriting :refer [function-registry register-function pmdfunctions]]
            [drafter.routes.sparql-update :refer [draft-update-endpoint-route state-update-endpoint-route live-update-endpoint-route raw-update-endpoint-route]]
            [drafter.configuration :as conf]
            [drafter.operations :as ops]
            [noir.util.middleware :refer [app-handler]]
            [compojure.route :as route]
            [selmer.parser :as parser]
            [drafter.rdf.draft-management :refer [graph-map lookup-live-graph-uri drafter-state-graph]]
            [drafter.operations :as operations]
            [drafter.rdf.sparql-protocol :as sproto]
            [grafter.rdf.repository :as repo]
            [compojure.handler :only [api]]
            [taoensso.timbre :as timbre]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clj-logging-config.log4j :refer [set-loggers!]])
  (:import [org.apache.log4j ConsoleAppender DailyRollingFileAppender EnhancedPatternLayout PatternLayout SimpleLayout]
           [org.apache.log4j.helpers DateLayout]))


(def default-repo-path "drafter-db")

;; Set these values later when we start the server
(def repo)
(def app)

(def worker)

(def state (atom {})) ; initialize state with an empty hashmap
(def stop-reaper (fn []))

(defmacro set-var-root! [var form]
  `(alter-var-root ~var (fn [& _#]
                         ~form)))

(defn register-sparql-extension-functions
  "Register custom drafter SPARQL extension functions."
  []

  ;; This function converts draft graphs into live graph URI's and is
  ;; necessary for drafters query/result rewriting to work.
  (register-function function-registry
                     (pmdfunctions "replace-live-graph")
                     (partial lookup-live-graph-uri repo)))

(defn initialise-repo! [repo-path indexes]
  (set-var-root! #'repo (let [repo (repo/repo (repo/native-store repo-path indexes))]
                          (log/info "Initialised repo" repo-path)
                          repo))

  (register-sparql-extension-functions))

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(def add-route conj)
(def add-routes into)

(defn- log-endpoint-config [ep-name endpoint-type config]
  (timbre/trace (str (name endpoint-type) " endpoint for route '" (name ep-name) "': " config)))

(defn create-sparql-endpoint-routes [route-name query-fn update-fn add-dumps? repo timeout-config]
  (let [query-path (str "/sparql/" (name route-name))
        update-path (str query-path "/update")
        query-timeouts (conf/get-endpoint-timeout route-name :query timeout-config)
        update-timeouts (conf/get-endpoint-timeout route-name :update timeout-config)
        query-route (query-fn query-path repo query-timeouts)
        update-route (update-fn update-path repo update-timeouts)
        routes [query-route update-route]]
    
    (log-endpoint-config route-name :query query-timeouts)
    (log-endpoint-config route-name :update update-timeouts)

    (if add-dumps?
      (let [dump-path (str "/data/" (name route-name))
            dump-fn #(query-fn %1 %2 query-timeouts)
            dump-route (dumps-endpoint dump-path dump-fn repo)]
        (conj routes dump-route))
      routes)))

(defn specify-endpoint [query-fn update-fn has-dump?]
  {:query-fn query-fn :update-fn update-fn :has-dump has-dump?})

(def live-endpoint-spec (specify-endpoint live-sparql-routes live-update-endpoint-route true))
(def raw-endpoint-spec (specify-endpoint raw-sparql-routes raw-update-endpoint-route true))
(def draft-endpoint-spec (specify-endpoint draft-sparql-routes draft-update-endpoint-route true))
(def state-endpoint-spec (specify-endpoint state-sparql-routes state-update-endpoint-route false))

(defn create-sparql-routes [endpoint-map repo]
  (let [default-timeouts ops/default-timeouts
        timeout-conf (conf/get-timeout-config env (keys endpoint-map) default-timeouts)
        ep-routes (fn [[ep-name {:keys [query-fn update-fn has-dump]}]]
                    (create-sparql-endpoint-routes ep-name query-fn update-fn has-dump repo timeout-conf))]
    (mapcat ep-routes endpoint-map)))

(defn get-sparql-routes [repo]
  (let [endpoints {:live live-endpoint-spec
                   :raw raw-endpoint-spec
                   :draft draft-endpoint-spec
                   :state state-endpoint-spec}]
    (create-sparql-routes endpoints repo)))

(defn initialise-app! [repo state]
  (set-var-root! #'app (app-handler
                        ;; add your application routes here
                        (-> []
                            (add-route (pages-routes repo))
                            (add-route (draft-api-routes "/draft" repo state))
                            (add-route (graph-management-routes "/graph" repo state))
                            (add-routes (get-sparql-routes repo))
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

(defn initialise-services! [repo-path indexes]
  (initialise-repo! repo-path indexes)
  (initialise-app! repo state)
  (set-var-root! #'stop-reaper (operations/start-reaper 2000)))

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

  (initialise-services! (get env :drafter-repo-path default-repo-path)
  ; http://sw.deri.org/2005/02/dexa/yars.pdf - see table on p5 for full coverage of indexes.
  ; (but we have to specify 4 char strings, so in some cases last chars don't matter
                    (get env :drafter-indexes "spoc,pocs,ocsp,cspo,cpos,oscp"))

  (log/info "drafter started successfully"))

(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (log/info "drafter is shutting down.  Please wait (this can take a minute)...")
  (repo/shutdown repo)
  (future-cancel worker)
  (stop-reaper)
  (log/info "drafter has shut down."))
