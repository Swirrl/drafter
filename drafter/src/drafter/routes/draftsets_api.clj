(ns drafter.routes.draftsets-api
  (:require [clj-logging-config.log4j :as l4j]
            [clojure.spec.alpha :as s]
            [compojure.core :as compojure :refer [context routes]]
            [drafter.backend.draftset :as ep]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [drafter.feature.middleware :as feat-middleware]
            [drafter.middleware
             :refer
             [negotiate-quads-content-type-handler
              negotiate-triples-content-type-handler]]
            [drafter.rdf.draftset-management.job-util :as jobutil]
            [drafter.rdf.sparql-protocol :as sp :refer [sparql-execution-handler]]
            [drafter.responses
             :refer
             [forbidden-response unprocessable-entity-response]]
            [drafter.user :as user]
            [drafter.util :as util]
            [integrant.core :as ig]
            [ring.util.response :as ring]
            [swirrl-server.responses :as response]))

(defn make-route [method route handler-fn]
  (compojure/make-route method route
                        (fn [req]
                          (l4j/with-logging-context {:method method
                                                     :route route}
                            (handler-fn req)))))

(defn draftset-api-routes [{:keys [get-users-handler
                                   get-draftsets-handler
                                   get-draftset-handler
                                   create-draftsets-handler
                                   delete-draftset-handler
                                   draftset-options-handler
                                   draftset-get-data-handler
                                   delete-draftset-data-handler
                                   delete-draftset-graph-handler
                                   delete-draftset-changes-handler
                                   put-draftset-data-handler
                                   put-draftset-graph-handler
                                   draftset-query-handler
                                   draftset-publish-handler
                                   draftset-set-metadata-handler
                                   draftset-submit-to-handler
                                   draftset-claim-handler]}]

  (let [version "/v1"]
    (context
     version []
     (routes
      (make-route :get "/users" get-users-handler)

      (make-route :get "/draftsets" get-draftsets-handler)

      (make-route :post "/draftsets" create-draftsets-handler)

      (make-route :get "/draftset/:id" get-draftset-handler)

      (make-route :delete "/draftset/:id" delete-draftset-handler)

      (make-route :options "/draftset/:id" draftset-options-handler)

      (make-route :get "/draftset/:id/data" draftset-get-data-handler)

      (make-route :delete "/draftset/:id/data" delete-draftset-data-handler)

      (make-route :delete "/draftset/:id/graph" delete-draftset-graph-handler)

      (make-route :delete "/draftset/:id/changes" delete-draftset-changes-handler)

      (make-route :put "/draftset/:id/data" put-draftset-data-handler)

      (make-route :put "/draftset/:id/graph" put-draftset-graph-handler)

      (make-route nil "/draftset/:id/query" draftset-query-handler)

      (make-route :post "/draftset/:id/publish" draftset-publish-handler)

      (make-route :put "/draftset/:id" draftset-set-metadata-handler)

      (make-route :post "/draftset/:id/submit-to" draftset-submit-to-handler)

      (make-route :put "/draftset/:id/claim" draftset-claim-handler)))))


(defmethod ig/pre-init-spec :drafter.routes/draftsets-api [_]
  (s/keys :req-un [;; handlers
                   ::get-draftsets-handler
                   ::create-draftsets-handler
                   ::get-draftset-handler
                   ::delete-draftset-handler
                   ::draftset-options-handler
                   ::draftset-get-data-handler
                   ::delete-draftset-data-handler
                   ::delete-draftset-graph-handler
                   ::delete-draftset-changes-handler
                   ::put-draftset-data-handler
                   ::put-draftset-graph-handler
                   ::draftset-query-handler
                   ::draftset-publish-handler
                   ::draftset-set-metadata-handler
                   ::draftset-submit-to-handler
                   ::draftset-claim-handler
                   ::get-users-handler]))

(defmethod ig/init-key :drafter.routes/draftsets-api [_ opts]
  (draftset-api-routes opts))
