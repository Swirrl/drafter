(ns drafter.routes.draftsets-api
  (:require [clj-logging-config.log4j :as l4j]
            [compojure.core :as compojure :refer [context routes]]
            [drafter.backend :as backend]
            [drafter.feature.common :as feat-common]
            [drafter.backend.draftset :as ep]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.draftset :as ds]
            [drafter.middleware
             :refer
             [negotiate-quads-content-type-handler
              negotiate-triples-content-type-handler
              optional-enum-param
              require-rdf-content-type
              temp-file-body]]
            [drafter.rdf.draftset-management.job-util :as jobutil :refer [make-job]]
            [drafter.rdf.draftset-management.jobs :as dsjobs]
            [drafter.rdf.sesame :refer [is-quads-format? is-triples-format?]]
            [drafter.rdf.sparql-protocol :refer [sparql-execution-handler sparql-protocol-handler] :as sp]
            [drafter.responses
             :refer
             [conflict-detected-response
              forbidden-response
              run-sync-job!
              submit-async-job!
              unprocessable-entity-response]]
            [drafter.user :as user]
            [integrant.core :as ig]
            [ring.util.response :as ring]
            [swirrl-server.async.jobs :as ajobs]
            [swirrl-server.responses :as response]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [drafter.util :refer [get-current-time]]
            [grafter.rdf4j.repository :as repo]
            [drafter.util :as util])
  (:import java.net.URI
           java.util.Date))

(defn- existing-draftset-handler [backend inner-handler]
  (fn [{{:keys [id]} :params :as request}]
    (let [draftset-id (ds/->DraftsetId id)]
      (if (dsops/draftset-exists? (repo/->connection backend) draftset-id)
        (let [updated-request (assoc-in request [:params :draftset-id] draftset-id)]
          (inner-handler updated-request))
        (ring/not-found "")))))

(defn- restrict-to-draftset-owner [backend inner-handler]
  (fn [{user :identity {:keys [draftset-id]} :params :as request}]
    (if (dsops/is-draftset-owner? backend draftset-id user)
      (inner-handler request)
      (forbidden-response "Operation only permitted by draftset owner"))))

(defn parse-query-param-flag-handler [flag inner-handler]
  (fn [{:keys [params] :as request}]
    (letfn [(update-request [b] (assoc-in request [:params flag] b))]
      (if-let [value (get params flag)]
        (if (boolean (re-matches #"(?i)(true|false)" value))
          (let [ub (Boolean/parseBoolean value)]
            (inner-handler (update-request ub)))
          (unprocessable-entity-response (str "Invalid " (name flag) " parameter value - expected true or false")))
        (inner-handler (update-request false))))))

(defn- parse-union-with-live-handler [inner-handler]
  (parse-query-param-flag-handler :union-with-live inner-handler))

;; (defn- run-sync
;;   ([api-call-fn]
;;    (run-sync-job! (as-sync-write-job api-call-fn)))
;;   ([api-call-fn resp-fn]
;;    (run-sync-job! (as-sync-write-job api-call-fn)
;;                      resp-fn)))

(defn- draftset-sync-write-response [result backend draftset-id]
  (if (jobutil/failed-job-result? result)
    (response/api-response 500 result)
    (ring/response (dsops/get-draftset-info backend draftset-id))))

(defn make-route [method route handler-fn]
  (compojure/make-route method route
                        (fn [req]
                          (l4j/with-logging-context
                            {:method method
                             :route route}
                            (handler-fn req)))))

(defn create-draftsets-handler [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (let [version "/v1"]
    (wrap-authenticated
     (fn [{{:keys [display-name description]} :params user :identity :as request}]
       (feat-common/run-sync #(dsops/create-draftset! backend user display-name description util/create-uuid util/get-current-time)
                 (fn [result]
                   (if (jobutil/failed-job-result? result)
                     (response/api-response 500 result)
                     (ring/redirect-after-post (str version "/draftset/"
                                                    (get-in result [:details :id]))))))))))

(s/def ::wrap-auth fn?)

(defmethod ig/pre-init-spec ::create-draftsets-handler [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-auth]))

(defmethod ig/init-key ::create-draftsets-handler [_ opts]
  (create-draftsets-handler opts))


(defn get-draftset-handler [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (wrap-authenticated
   (existing-draftset-handler
    backend
    (fn [{{:keys [draftset-id]} :params user :identity :as request}]
      (if-let [info (dsops/get-draftset-info backend draftset-id)]
        (if (user/can-view? user info)
          (ring/response info)
          (forbidden-response "Draftset not in accessible state"))
        (ring/not-found ""))))))

(defmethod ig/pre-init-spec ::get-draftset-handler [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-auth]))

(defmethod ig/init-key ::get-draftset-handler [_ opts]
  (get-draftset-handler opts))

(defn wrap-as-draftset-owner [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (fn [handler]
    (wrap-authenticated
     (existing-draftset-handler
      backend
      (restrict-to-draftset-owner backend handler)))))

(defmethod ig/pre-init-spec ::wrap-as-draftset-owner [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-auth]))

(defmethod ig/init-key ::wrap-as-draftset-owner [_ opts]
  (wrap-as-draftset-owner opts))

(defn delete-draftset-handler [{wrap-as-draftset-owner :wrap-as-draftset-owner backend :drafter/backend}]
  (log/info "del draftset handler wrapper: " wrap-as-draftset-owner)
  (wrap-as-draftset-owner
   (fn [request]
     (log/info "delete-draftset-handler " request)
     (let [{{:keys [draftset-id]} :params :as request} request]
       (submit-async-job! (dsjobs/delete-draftset-job backend draftset-id))))))

(defmethod ig/pre-init-spec ::delete-draftset-handler [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::delete-draftset-handler [_ opts]
  (delete-draftset-handler opts))


(defn draftset-options-handler [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (wrap-authenticated
   (existing-draftset-handler
    backend
    (fn [{{:keys [draftset-id]} :params user :identity}]
      (let [permitted (dsops/find-permitted-draftset-operations backend draftset-id user)]
        (ring/response permitted))))))

(defmethod ig/pre-init-spec ::draftset-options-handler [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-auth]))

(defmethod ig/init-key ::draftset-options-handler [_ opts]
  (draftset-options-handler opts))

(defn draftset-get-data-handler [{wrap-as-draftset-owner :wrap-as-draftset-owner backend :drafter/backend
                                  draftset-query-timeout-fn :timeout-fn}]
  (wrap-as-draftset-owner
   (parse-union-with-live-handler
    (fn [{{:keys [draftset-id graph union-with-live] :as params} :params :as request}]
      (let [;;executor (ep/draftset-endpoint {:backend backend :draftset-ref draftset-id :union-with-live? union-with-live})
            executor (ep/build-draftset-endpoint backend draftset-id union-with-live)
            is-triples-query? (contains? params :graph)
            conneg (if is-triples-query?
                     negotiate-triples-content-type-handler
                     negotiate-quads-content-type-handler)
            pquery (if is-triples-query?
                     (dsops/all-graph-triples-query executor graph)
                     (dsops/all-quads-query executor))
            handler (->> sparql-execution-handler
                         (sp/sparql-timeout-handler draftset-query-timeout-fn)
                         (conneg)
                         (sp/sparql-constant-prepared-query-handler pquery))]
        (handler request))))))

(defmethod ig/pre-init-spec ::draftset-get-data-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner ::sp/timeout-fn]))

(defmethod ig/init-key ::draftset-get-data-handler [_ opts]
  (draftset-get-data-handler opts))

;; (defn delete-draftset-data-handler [{wrap-as-draftset-owner :wrap-as-draftset-owner backend :drafter/backend}]
;;   (wrap-as-draftset-owner
;;    (require-rdf-content-type
;;     (require-graph-for-triples-rdf-format
;;      (temp-file-body
;;       (fn [{{draftset-id :draftset-id
;;             graph :graph
;;             rdf-format :rdf-format} :params body :body :as request}]
;;         (let [delete-job (if (is-quads-format? rdf-format)
;;                            (dsjobs/delete-quads-from-draftset-job backend draftset-id body rdf-format util/get-current-time)
;;                            (dsjobs/delete-triples-from-draftset-job backend draftset-id graph body rdf-format util/get-current-time))]
;;           (submit-async-job! delete-job))))))))

;; (defmethod ig/pre-init-spec ::delete-draftset-data-handler [_]
;;   (s/keys :req [:drafter/backend]
;;           :req-un [::wrap-as-draftset-owner]))

;; (defmethod ig/init-key ::delete-draftset-data-handler [_ opts]
;;   (delete-draftset-data-handler opts))



;; (defn delete-draftset-changes-handler [{backend :drafter/backend wrap-as-draftset-owner :wrap-as-draftset-owner}]
;;   (wrap-as-draftset-owner
;;    (parse-graph-param-handler
;;     true
;;     (fn [{{:keys [draftset-id graph]} :params}]
;;       (run-sync #(dsops/revert-graph-changes! backend draftset-id graph)
;;                 (fn [result]
;;                   (if (jobutil/failed-job-result? result)
;;                     (response/api-response 500 result)
;;                     (if (= :reverted (:details result))
;;                       (ring/response (dsops/get-draftset-info backend draftset-id))
;;                       (ring/not-found "")))))))))

;; (defmethod ig/pre-init-spec ::delete-draftset-changes-handler [_]
;;   (s/keys :req [:drafter/backend]
;;           :req-un [::wrap-as-draftset-owner]))

;; (defmethod ig/init-key ::delete-draftset-changes-handler [_ opts]
;;   (delete-draftset-changes-handler opts))

;; (defn put-draftset-data-handler [{backend :drafter/backend wrap-as-draftset-owner :wrap-as-draftset-owner}]
;;   (wrap-as-draftset-owner
;;    (require-rdf-content-type
;;     (require-graph-for-triples-rdf-format
;;      (temp-file-body
;;       (fn [{{draftset-id :draftset-id
;;             rdf-format :rdf-format
;;             graph :graph} :params body :body :as request}]
;;         (if (is-quads-format? rdf-format)
;;           (let [append-job (dsjobs/append-data-to-draftset-job backend draftset-id body rdf-format util/get-current-time)]
;;             (submit-async-job! append-job))
;;           (let [append-job (dsjobs/append-triples-to-draftset-job backend draftset-id body rdf-format graph util/get-current-time)]
;;             (submit-async-job! append-job)))))))))

;; (defmethod ig/pre-init-spec ::put-draftset-data-handler [_]
;;   (s/keys :req [:drafter/backend]
;;           :req-un [::wrap-as-draftset-owner]))

;; (defmethod ig/init-key ::put-draftset-data-handler [_ opts]
;;   (put-draftset-data-handler opts))

;; (defn put-draftset-graph-handler [{backend :drafter/backend
;;                                    :keys [wrap-as-draftset-owner]}]
;;   (letfn [(required-live-graph-param [handler]
;;             (parse-graph-param-handler true (required-live-graph-param-handler backend handler)))]
    
;;     (wrap-as-draftset-owner
;;      (required-live-graph-param
;;       (fn [{{:keys [draftset-id graph]} :params}]
;;         (submit-async-job! (dsjobs/copy-live-graph-into-draftset-job backend draftset-id graph)))))))

;; (defmethod ig/pre-init-spec ::put-draftset-graph-handler [_]
;;   (s/keys :req [:drafter/backend]
;;           :req-un [::wrap-as-draftset-owner]))

;; (defmethod ig/init-key ::put-draftset-graph-handler [_ opts]
;;   (put-draftset-graph-handler opts))


;;;;; !!!! TODO TODO TEST THIS WITH STASHER !!!!
;;;;; !!!! TODO TODO TEST THIS WITH STASHER !!!!
;;;;; !!!! TODO TODO TEST THIS WITH STASHER !!!!
;;;;; !!!! TODO TODO TEST THIS WITH STASHER !!!!
(defn draftset-query-handler [{backend :drafter/backend
                               :keys [wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (parse-union-with-live-handler
    (fn [{{:keys [draftset-id union-with-live]} :params :as request}]
      (let [executor (backend/endpoint-repo backend draftset-id {:union-with-live? union-with-live})
            handler (sparql-protocol-handler {:repo executor :timeout-fn timeout-fn})]
        (handler request))))))

(defmethod ig/pre-init-spec ::draftset-query-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner ::sp/timeout-fn]))

(defmethod ig/init-key ::draftset-query-handler [_ opts]
  (draftset-query-handler opts))

(defn draftset-publish-handler [{backend :drafter/backend
                                 :keys [wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (fn [{{:keys [draftset-id]} :params user :identity}]
     (if (user/has-role? user :publisher)
       (submit-async-job! (dsjobs/publish-draftset-job backend draftset-id util/get-current-time))
       (forbidden-response "You require the publisher role to perform this action")))))

(defmethod ig/pre-init-spec ::draftset-publish-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::draftset-publish-handler [_ opts]
  (draftset-publish-handler opts))

(defn draftset-set-metadata-handler [{backend :drafter/backend
                                      :keys [wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (fn [{{:keys [draftset-id] :as params} :params}]
     (feat-common/run-sync #(dsops/set-draftset-metadata! backend draftset-id params)
               #(draftset-sync-write-response % backend draftset-id)))))

(defmethod ig/pre-init-spec ::draftset-set-metadata-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::draftset-set-metadata-handler [_ opts]
  (draftset-set-metadata-handler opts))

(defn draftset-submit-to-handler [{backend :drafter/backend
                                   user-repo ::user/repo
                                   :keys [wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (fn [{{:keys [user role draftset-id]} :params owner :identity}]
     (cond
       (and (some? user) (some? role))
       (unprocessable-entity-response "Only one of user and role parameters permitted")

       (some? user)
       (if-let [target-user (user/find-user-by-username user-repo user)]
         (feat-common/run-sync #(dsops/submit-draftset-to-user! backend draftset-id owner target-user)
                   #(draftset-sync-write-response % backend draftset-id))
         (unprocessable-entity-response (str "User: " user " not found")))

       (some? role)
       (let [role-kw (keyword role)]
         (if (user/is-known-role? role-kw)
           (feat-common/run-sync #(dsops/submit-draftset-to-role! backend draftset-id owner role-kw)
                     #(draftset-sync-write-response % backend draftset-id))
           (unprocessable-entity-response (str "Invalid role: " role))))

       :else
       (unprocessable-entity-response (str "user or role parameter required"))))))

(defmethod ig/pre-init-spec ::draftset-submit-to-handler [_]
  (s/keys :req [:drafter/backend ::user/repo]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::draftset-submit-to-handler [_ opts]
  (draftset-submit-to-handler opts))

(defn draftset-claim-handler [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (wrap-authenticated
   (existing-draftset-handler
    backend
    (fn [{{:keys [draftset-id]} :params user :identity}]
      (if-let [ds-info (dsops/get-draftset-info backend draftset-id)]
        (if (user/can-claim? user ds-info)
          (feat-common/run-sync #(dsops/claim-draftset! backend draftset-id user)
                    (fn [result]
                      (if (jobutil/failed-job-result? result)
                        (response/api-response 500 result)
                        (let [[claim-outcome ds-info] (:details result)]
                          (if (= :ok claim-outcome)
                            (ring/response ds-info)
                            (conflict-detected-response "Failed to claim draftset"))))))
          (forbidden-response "User not in role for draftset claim"))
        (ring/not-found "Draftset not found"))))))


(defmethod ig/pre-init-spec ::draftset-claim-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-auth]))

(defmethod ig/init-key ::draftset-claim-handler [_ opts]
  (draftset-claim-handler opts))


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
 
