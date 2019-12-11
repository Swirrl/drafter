(ns drafter.routes.draftsets-api
  (:require [clj-logging-config.log4j :as l4j]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [compojure.core :as compojure :refer [context routes]]
            [drafter.backend :as backend]
            [drafter.backend.draftset :as ep]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [drafter.feature.middleware :as feat-middleware]
            [drafter.middleware
             :refer
             [negotiate-quads-content-type-handler
              negotiate-triples-content-type-handler]]
            [drafter.rdf.draftset-management.job-util :as jobutil]
            [drafter.rdf.draftset-management.jobs :as dsjobs]
            [drafter.rdf.sparql-protocol :as sp
             :refer [sparql-execution-handler sparql-protocol-handler]]
            [drafter.responses
             :refer
             [conflict-detected-response
              forbidden-response
              submit-async-job!
              unprocessable-entity-response]]
            [drafter.user :as user]
            [drafter.util :as util]
            [integrant.core :as ig]
            [ring.util.response :as ring]
            [drafter.async.responses :as response]
            [drafter.requests :as req]))

(defn parse-query-param-flag-handler [flag inner-handler]
  (fn [{:keys [params] :as request}]
    (letfn [(update-request [b] (assoc-in request [:params flag] b))]
      (if-let [value (get params flag)]
        (if (boolean (re-matches #"(?i)(true|false)" value))
          (let [ub (Boolean/parseBoolean value)]
            (inner-handler (update-request ub)))
          (unprocessable-entity-response (str "Invalid " (name flag) " parameter value - expected true or false")))
        (inner-handler (update-request false))))))

(defn parse-union-with-live-handler [inner-handler]
  (parse-query-param-flag-handler :union-with-live inner-handler))

(defn make-route [method route handler-fn]
  (compojure/make-route method route
                        (fn [req]
                          (l4j/with-logging-context
                            {:method method
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
                                   delete-draftset-graph-async-handler
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
