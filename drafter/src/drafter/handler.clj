(ns drafter.handler
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [cognician.dogstatsd :as datadog]
            [compojure.core :refer [context defroutes GET]]
            [compojure.route :as route]
            [drafter.backend.common :refer [stop-backend]]
            [drafter.env :as denv]
            [drafter.middleware :as middleware]
            [drafter.routes.pages :refer [pages-routes]]
            [drafter.routes.status :refer [status-routes]]
            [drafter.swagger :as swagger]
            [drafter.timeouts :as timeouts]
            [drafter.util :refer [conj-if set-var-root!]]
            [drafter.write-scheduler
             :refer [start-writer! stop-writer!]]
            [integrant.core :as ig]
            [noir.util.middleware :refer [app-handler]]
            [ring.middleware.defaults :refer [api-defaults]]
            [ring.middleware.file-info :refer [wrap-file-info]]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.verbs :refer [wrap-verbs]]
            [swirrl-server.errors :refer [wrap-encode-errors]]
            [swirrl-server.middleware.log-request :refer [log-request]]))

(defroutes app-routes
  (GET "/swagger/swagger.json" []
       {:status 200
        :headers {"Content-Type" "application/json"}
        :body (swagger/load-spec-and-resolve-refs)})
  (route/resources "/")
  (route/not-found "Not Found"))

(def add-route conj)
(def add-routes into)

(defn- wrap-handler [handler]
  (-> handler
      ;; Makes static assets in $PROJECT_DIR/resources/public/ available.
      (wrap-resource "public")

      ;; Content-Type, Content-Length, and Last Modified headers for files in body
      wrap-file-info))

(defn- build-handler
  [{backend :repo
    live-sparql-route :live-sparql-query-route
    draftset-api-routes :draftset-api-routes
    jobs-status-routes :jobs-status-routes
    global-writes-lock :drafter/global-writes-lock}]
  (wrap-handler (app-handler
                 ;; add your application routes here
                 (-> []
                     (add-route (pages-routes))
                     (add-route draftset-api-routes)
                     (add-route live-sparql-route)
                     (add-route (context "/v1/status" [] (status-routes global-writes-lock)))
                     (add-route jobs-status-routes)
                     (add-routes (denv/env-specific-routes backend))
                     (add-route app-routes))

                 :ring-defaults (assoc-in api-defaults [:params :multipart] true)
                 ;; add custom middleware here
                 :middleware [#(wrap-resource % "swagger-ui")
                              wrap-verbs
                              wrap-encode-errors
                              middleware/wrap-total-requests-counter
                              middleware/wrap-request-timer
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
