(ns drafter.handler
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [compojure
             [core :refer [context defroutes]]
             [route :as route]]
            [drafter
             [env :as denv]
             [configuration :as conf]
             [middleware :as middleware]
             [util :refer [set-var-root!]]
             [write-scheduler :refer [global-writes-lock start-writer! stop-writer!]]]
            [drafter.backend.protocols :refer [stop-backend]]
            [drafter.backend.sesame.remote :refer [get-backend]]
            [drafter.common.json-encoders :as enc]
            [drafter.routes
             [draftsets-api :refer [draftset-api-routes]]
             [pages :refer [pages-routes]]
             [sparql :refer [live-sparql-routes]]
             [status :refer [status-routes]]]
            [environ.core :refer [env]]
            [noir.util.middleware :refer [app-handler]]
            [ring.middleware
             [defaults :refer [api-defaults]]
             [resource :refer [wrap-resource]]
             [verbs :refer [wrap-verbs]]]
            [selmer.parser :as parser]
            [swirrl-server.async.jobs :refer [finished-jobs restart-id]]
            [swirrl-server.errors :refer [wrap-encode-errors]]
            [swirrl-server.middleware.log-request :refer [log-request]]))

;; Note that though the classes and requires below aren't used in this
;; namespace they are needed by the log-config file which is loaded
;; from here.
(import '[org.apache.log4j ConsoleAppender DailyRollingFileAppender RollingFileAppender EnhancedPatternLayout PatternLayout SimpleLayout]
        '[org.apache.log4j.helpers DateLayout]
        '[java.util UUID])

(require '[clj-logging-config.log4j :refer [set-loggers!]]
         '[drafter.timeouts :as timeouts]
         'drafter.errors)

;; Set these values later when we start the server
(def backend)
(def user-repo)
(def app)

(def ^{:doc "A future to control the single write thread that performs database writes."}
  writer-service)

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(def add-route conj)
(def add-routes into)

(defn- endpoint-query-path [route-name version]
  (let [suffix (str "/sparql/" (name route-name))]
    (if (some? version)
      (str "/" (name version) suffix)
      suffix)))

(defn- get-sparql-endpoint-timeout-config []
  (conf/get-timeout-config env #{:live :draftset}))

(def ^:private v1-prefix :v1)

(defn- get-endpoint-query-timeout-fn [endpoint-timeouts]
  (let [signing-key (:drafter-jws-signing-key env)]
    (when (nil? signing-key)
      (log/warn "DRAFTER_JWS_SIGNING_KEY not configured - any PMD query timeouts will be ignored"))
    (timeouts/calculate-request-query-timeout endpoint-timeouts signing-key)))

(defn- get-live-sparql-query-route [backend timeouts-config]
  (let [live-timeouts (conf/get-endpoint-timeout :live :query timeouts-config)
        timeout-fn (get-endpoint-query-timeout-fn live-timeouts)
        mount-point (endpoint-query-path :live v1-prefix)]
    (live-sparql-routes mount-point backend timeout-fn)))

(defn initialise-app! [backend]
  (let [authenticated-fn (middleware/make-authenticated-wrapper user-repo env)
        sparql-timeouts-config (get-sparql-endpoint-timeout-config)
        draftset-sparql-query-timeout (conf/get-endpoint-timeout :draftset :query sparql-timeouts-config)
        draftset-sparql-query-timeout-fn (get-endpoint-query-timeout-fn draftset-sparql-query-timeout)]
    (set-var-root! #'app (app-handler
                          ;; add your application routes here
                          (-> []
                              (add-route (pages-routes))
                              (add-route (draftset-api-routes backend user-repo authenticated-fn draftset-sparql-query-timeout-fn))
                              (add-route (get-live-sparql-query-route backend sparql-timeouts-config))
                              ;;(add-routes (get-sparql-routes backend authenticated-fn sparql-timeouts-config))
                              (add-route (context "/v1/status" []
                                                  (status-routes global-writes-lock finished-jobs restart-id)))

                              (add-routes (denv/env-specific-routes backend))
                              (add-route app-routes))

                          :ring-defaults (assoc-in api-defaults [:params :multipart] true)
                          ;; add custom middleware here
                          :middleware [#(wrap-resource % "swagger-ui")
                                       wrap-verbs
                                       wrap-encode-errors
                                       log-request]
                          ;; add access rules here
                          :access-rules []
                          ;; serialize/deserialize the following data formats
                          ;; available formats:
                          ;; :json :json-kw :yaml :yaml-kw :edn :yaml-in-html
                          :formats [:json-kw :edn]))))

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
  (initialise-app! backend))

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
  (stop-backend backend)
  (.close user-repo)
  (stop-writer! writer-service)
  (log/info "drafter has shut down."))
