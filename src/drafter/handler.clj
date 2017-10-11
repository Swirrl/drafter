(ns drafter.handler
  (:require [clojure.java.io :as io]
            [cognician.dogstatsd :as datadog]
            [clojure.tools.logging :as log]
            [compojure.core :refer [defroutes context]]
            [compojure.route :as route]
            [drafter.middleware :as middleware]
            [swirrl-server.middleware.log-request :refer [log-request]]
            [drafter.backend.protocols :refer [stop-backend]]
            [drafter.backend.sesame.remote :refer [get-backend]]
            [drafter.util :refer [set-var-root! conj-if]]
            [drafter.common.json-encoders :as enc]
            [drafter.routes.draftsets-api :refer [draftset-api-routes]]
            [drafter.routes.status :refer [status-routes]]
            [drafter.routes.pages :refer [pages-routes]]
            [drafter.routes.sparql :refer [live-sparql-routes]]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.write-scheduler :refer [start-writer!
                                             global-writes-lock
                                             stop-writer!]]
            [drafter.configuration :refer [get-configuration]]
            [drafter.env :as denv]
            [drafter.rdf.writers :as writers]
            [swirrl-server.async.jobs :refer [restart-id finished-jobs]]
            [noir.util.middleware :refer [app-handler]]
            [ring.middleware
             [defaults :refer [api-defaults]]
             [resource :refer [wrap-resource]]
             [verbs :refer [wrap-verbs]]]
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

(def writer-service
  "A future to control the single write thread that performs database writes.")

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

(def ^:private v1-prefix :v1)

(defn- get-endpoint-query-timeout-fn [endpoint-timeout {:keys [jws-signing-key] :as config}]
  (when (nil? jws-signing-key)
    (log/warn "jws-signing-key not configured - any PMD query timeouts will be ignored"))
  (timeouts/calculate-request-query-timeout endpoint-timeout jws-signing-key))

(defn- get-live-sparql-query-route [backend {:keys [live-query-timeout] :as config}]
  (let [timeout-fn (get-endpoint-query-timeout-fn live-query-timeout config)
        mount-point (endpoint-query-path :live v1-prefix)]
    (live-sparql-routes mount-point backend timeout-fn)))

(defn initialise-app! [backend {:keys [draftset-query-timeout] :as config}]
  (let [authenticated-fn (middleware/make-authenticated-wrapper user-repo config)
        draftset-sparql-query-timeout-fn (get-endpoint-query-timeout-fn draftset-query-timeout config)]
    (set-var-root! #'app (app-handler
                          ;; add your application routes here
                          (-> []
                              (add-route (pages-routes))
                              (add-route (draftset-api-routes backend user-repo authenticated-fn draftset-sparql-query-timeout-fn))
                              (add-route (get-live-sparql-query-route backend config))
                              (add-route (context "/v1/status" []
                                                  (status-routes global-writes-lock finished-jobs restart-id)))

                              (add-routes (denv/env-specific-routes backend))
                              (add-route app-routes))

                          :ring-defaults (assoc-in api-defaults [:params :multipart] true)
                          ;; add custom middleware here
                          :middleware [#(wrap-resource % "swagger-ui")
                                       wrap-verbs
                                       wrap-encode-errors
                                       middleware/wrap-total-requests-counter
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
  [config]
  (set-var-root! #'backend (get-backend config)))

(defn- init-user-repo! [config]
  (let [repo (drafter.user.repository/get-configured-repository config)]
    (set-var-root! #'user-repo repo)))

(defn initialise-services! [config]
  (enc/register-custom-encoders!)
  (writers/register-custom-rdf-writers!)

  (jobs/init-job-settings! config)
  (initialise-write-service!)
  (init-backend! config)
  (init-user-repo! config)
  (initialise-app! backend config))

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

  (let [{:keys [log-config-file is-dev datadog-statsd-address] :as config} (get-configuration)]

    (configure-logging! (io/file log-config-file))

    (log/info "Initialising repository")
    (initialise-services! config)

    (datadog/configure! datadog-statsd-address {:service :drafter})
    (log/info "drafter started successfully")))

(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (log/info "drafter is shutting down.  Please wait (this can take a minute)...")
  (stop-backend backend)
  (.close user-repo)
  (stop-writer! writer-service)
  (log/info "drafter has shut down."))
