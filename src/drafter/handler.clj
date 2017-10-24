(ns drafter.handler
  (:require [clojure.java.io :as io]
            [integrant.core :as ig]
            [cognician.dogstatsd :as datadog]
            [clojure.tools.logging :as log]
            [compojure.core :refer [defroutes context]]
            [compojure.route :as route]
            [drafter.middleware :as middleware]
            [swirrl-server.middleware.log-request :refer [log-request]]
            [drafter.backend.protocols :refer [stop-backend]]
            [drafter.util :refer [set-var-root! conj-if]]
            [drafter.routes.status :refer [status-routes]] ;; TODO componentise
            [drafter.routes.pages :refer [pages-routes]]   ;; TODO componentise
            [drafter.write-scheduler :refer [start-writer!
                                             global-writes-lock
                                             stop-writer!]]
            [drafter.timeouts :as timeouts]
            [drafter.env :as denv]
            [swirrl-server.async.jobs :refer [restart-id finished-jobs]]
            [noir.util.middleware :refer [app-handler]]
            [ring.middleware.file-info :refer [wrap-file-info]]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware
             [defaults :refer [api-defaults]]
             [resource :refer [wrap-resource]]
             [verbs :refer [wrap-verbs]]]
            [swirrl-server.async.jobs :refer [finished-jobs restart-id]]
            [swirrl-server.errors :refer [wrap-encode-errors]]
            [swirrl-server.middleware.log-request :refer [log-request]]))

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(def add-route conj)
(def add-routes into)

(defn- get-endpoint-query-timeout-fn [{:keys [endpoint-timeout jws-signing-key] :as config}]
  (when (nil? jws-signing-key)
    (log/warn "jws-signing-key not configured - any PMD query timeouts will be ignored"))
  (timeouts/calculate-request-query-timeout endpoint-timeout jws-signing-key))

(defmethod ig/init-key :drafter.handler/live-endpoint-timeout-fn [_ opts]
  (get-endpoint-query-timeout-fn opts))

(defmethod ig/init-key :drafter.handler/draftset-query-timeout-fn [_ opts]
  (get-endpoint-query-timeout-fn opts))

(defn- wrap-handler [handler]
  (-> handler
      ;; Makes static assets in $PROJECT_DIR/resources/public/ available.
      (wrap-resource "public")

      ;; Content-Type, Content-Length, and Last Modified headers for files in body
      wrap-file-info))

(defn- build-handler
  [{backend :repo
    authenticated-fn :authentication-handler
    draftset-sparql-query-timeout-fn :query-timeout-fn
    live-sparql-route :live-sparql-query-route
    draftset-api-routes :draftset-api-routes
    :as config}]
  (wrap-handler (app-handler 
                 ;; add your application routes here
                 (-> []
                     (add-route (pages-routes))
                     (add-route draftset-api-routes)
                     (add-route live-sparql-route)
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
                              log-request
                              ;;wrap-file-info       ;; Content-Type, Content-Length, and Last Modified headers for files in body
                              ]
                 ;; add access rules here
                 :access-rules []
                 ;; serialize/deserialize the following data formats
                 ;; available formats:
                 ;; :json :json-kw :yaml :yaml-kw :edn :yaml-in-html
                 :formats [:json-kw :edn])))


(defmethod ig/init-key :drafter.handler/app [k opts]
  (build-handler opts))

;; TODO remove/replace
#_(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (log/info "drafter is shutting down.  Please wait (this can take a minute)...")
  (stop-backend backend)
  (log/info "drafter has shut down."))
