(ns drafter.handler
  (:require [compojure.core :refer [context defroutes GET]]
            [compojure.route :as route]
            [drafter.env :as denv]
            [drafter.middleware :as middleware]
            [drafter.routes.status :refer [status-routes]]
            [drafter.swagger :as swagger]

            [drafter.timeouts :as timeouts]
            [drafter.util :refer [set-var-root!]]
            [drafter.write-scheduler :refer [start-writer! stop-writer!]]
            [integrant.core :as ig]
            [noir.util.middleware :refer [app-handler]]
            [ring.middleware.defaults :refer [api-defaults]]
            [ring.middleware.file-info :refer [wrap-file-info]]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.verbs :refer [wrap-verbs]]
            [drafter.errors :refer [wrap-encode-errors]]
            [drafter.logging :refer [log-request]]))

(defroutes app-routes
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
    global-writes-lock :drafter/global-writes-lock
    wrap-authenticate :wrap-authenticate
    swagger-routes :swagger-routes
    global-auth? :global-auth?}]
  (wrap-handler
   (app-handler
    ;; add your application routes here
    (-> []
        (add-route swagger-routes)
        (add-route draftset-api-routes)
        (add-route live-sparql-route)
        (add-route (context "/v1/status" [] (status-routes global-writes-lock)))
        (add-route jobs-status-routes)
        (add-routes (denv/env-specific-routes backend))
        (add-route app-routes))

    :ring-defaults (-> (assoc-in api-defaults [:params :multipart] true)
                       ;; Enables wrap-forwarded-scheme middleware. Essential in prod
                       ;; env when scheme needs to be passed through from load balancer
                       (assoc :proxy true))
    ;; add custom middleware here
    :middleware
    (let [middleware [wrap-verbs
                      wrap-encode-errors
                      middleware/wrap-total-requests-counter
                      middleware/wrap-request-timer
                      #(log-request % {:query "<scrubbed>"})]]
      (if global-auth?
        (cons #(middleware/wrap-authorize
                wrap-authenticate :public:view %)
              middleware)
        middleware))

    ;; add access rules here
    :access-rules []
    ;; serialize/deserialize the following data formats
    ;; available formats:
    ;; :json :json-kw :yaml :yaml-kw :edn :yaml-in-html
    :formats [:json-kw :edn])))

(defmethod ig/init-key :drafter/global-auth? [_ v] v)

(defmethod ig/init-key :drafter.handler/app [k opts]
  (build-handler opts))
