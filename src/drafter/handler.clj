(ns drafter.handler
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [compojure.core :refer [defroutes context]]
            [compojure.route :as route]
            [drafter.operations :as ops]
            [drafter.middleware :as middleware]
            [drafter.configuration :as conf]
            [drafter.backend.protocols :refer [stop]]
            [drafter.backend.configuration :refer [get-backend]]
            [drafter.util :refer [set-var-root!]]
            [drafter.common.json-encoders :as enc]
            [drafter.routes.draftsets-api :refer [draftset-api-routes]]
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
            [noir.util.middleware :refer [app-handler]]
            [ring.middleware.verbs :refer [wrap-verbs]]
            [ring.middleware.defaults :refer [api-defaults]]
            [swirrl-server.errors :refer [wrap-encode-errors]]
            [ring.middleware.resource :refer [wrap-resource]]
            [selmer.parser :as parser]
            [clojure.string :as str]
            [drafter.user.mongo :as mongousers])

  ;; Note that though the classes and requires below aren't used in this namespace
  ;; they are needed by the log-config file which is loaded from here.
  (:import [org.apache.log4j ConsoleAppender DailyRollingFileAppender RollingFileAppender EnhancedPatternLayout PatternLayout SimpleLayout]
           [org.apache.log4j.helpers DateLayout]
           [java.util UUID])

  (:require [clj-logging-config.log4j :refer [set-loggers!]]))

(require 'drafter.errors)

;; Set these values later when we start the server
(def backend)
(def user-repo)
(def app)

(def ^{:doc "A future to control the single write thread that performs database writes."}
  writer-service)

(def stop-reaper (fn []))

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

(defn- create-dumps-route [route-name query-fn backend timeout-config]
  (let [dump-path (str "/data/" (name route-name))
        query-timeouts (conf/get-endpoint-timeout route-name :query timeout-config)
        dump-fn #(query-fn %1 %2 query-timeouts)]
    (dumps-endpoint dump-path dump-fn backend)))

(defn create-sparql-endpoint-routes [route-name query-fn update-fn add-dumps? backend timeout-config]
  (let [query-route (endpoint-route route-name endpoint-query-path query-fn :query backend timeout-config)
        update-route (and update-fn (endpoint-route route-name endpoint-update-path update-fn :update backend timeout-config))
        dumps-route (when add-dumps?
                      (create-dumps-route route-name query-fn backend timeout-config))
        routes [query-route update-route]]
    (vec (remove nil? [query-route update-route dumps-route]))))

(defn specify-endpoint [query-fn update-fn has-dump?]
  {:query-fn query-fn :update-fn update-fn :has-dump has-dump?})

(def live-endpoint-spec (specify-endpoint live-sparql-routes live-update-endpoint-route true))
(def raw-endpoint-spec (specify-endpoint raw-sparql-routes raw-update-endpoint-route true))
(def draft-endpoint-spec (specify-endpoint draft-sparql-routes nil true))
(def state-endpoint-spec (specify-endpoint state-sparql-routes state-update-endpoint-route false))

(defn create-sparql-routes [endpoint-map backend]
  (let [timeout-conf (conf/get-timeout-config env (keys endpoint-map) ops/default-timeouts)
        ep-routes (fn [[ep-name {:keys [query-fn update-fn has-dump]}]]
                    (create-sparql-endpoint-routes ep-name query-fn update-fn has-dump backend timeout-conf))]
    (mapcat ep-routes endpoint-map)))

(defn get-sparql-routes [backend]
  (let [endpoints {:live live-endpoint-spec
                   :raw raw-endpoint-spec
                   :draft draft-endpoint-spec
                   :state state-endpoint-spec}]
    (create-sparql-routes endpoints backend)))

(defn initialise-app! [backend]
  (set-var-root! #'app (app-handler
                        ;; add your application routes here
                        (-> []
                            (add-route (pages-routes backend))
                            (add-route (draftset-api-routes backend user-repo "Drafter"))
                            (add-routes (get-sparql-routes backend))
                            (add-route (context "/status" []
                                                (status-routes global-writes-lock finished-jobs restart-id)))

                            (add-route app-routes))

                        :ring-defaults (assoc-in api-defaults [:params :multipart] true)
                        ;; add custom middleware here
                        :middleware [#(wrap-resource % "swagger-ui")
                                     wrap-verbs
                                     wrap-encode-errors
                                     middleware/log-request]
                        ;; add access rules here
                        :access-rules []
                        ;; serialize/deserialize the following data formats
                        ;; available formats:
                        ;; :json :json-kw :yaml :yaml-kw :edn :yaml-in-html
                        :formats [:json-kw :edn])))

(defn initialise-write-service! []
  (set-var-root! #'writer-service  (start-writer!)))

(defn- init-backend!
  "Creates the backend for the current configuration and sets the
  backend var."
  []
  (set-var-root! #'backend (get-backend env)))

(defn- init-user-repo! []
  (let [repo (drafter.user.repository/get-configured-repository env)]
    (set-var-root! #'user-repo repo)))

(defn initialise-services! []
  (enc/register-custom-encoders!)

  (initialise-write-service!)
  (init-backend!)
  (init-user-repo!)
  (initialise-app! backend)
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
  (initialise-services!)

  (log/info "drafter started successfully"))

(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (log/info "drafter is shutting down.  Please wait (this can take a minute)...")
  (stop backend)
  (.close user-repo)
  (stop-writer! writer-service)
  (stop-reaper)
  (log/info "drafter has shut down."))
