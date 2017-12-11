(ns drafter.routes.draftsets-api
  (:require [clj-logging-config.log4j :as l4j]
            [compojure.core :as compojure :refer [context routes]]
            [drafter.backend :as backend]
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
            [clojure.tools.logging :as log])
  (:import java.net.URI))

(defn- existing-draftset-handler [backend inner-handler]
  (fn [{{:keys [id]} :params :as request}]
    (let [draftset-id (ds/->DraftsetId id)]
      (if (dsops/draftset-exists? backend draftset-id)
        (let [updated-request (assoc-in request [:params :draftset-id] draftset-id)]
          (inner-handler updated-request))
        (ring/not-found "")))))

(defn- restrict-to-draftset-owner [backend inner-handler]
  (fn [{user :identity {:keys [draftset-id]} :params :as request}]
    (if (dsops/is-draftset-owner? backend draftset-id user)
      (inner-handler request)
      (forbidden-response "Operation only permitted by draftset owner"))))

(defn- try-parse-uri [s]
  (try
    (URI. s)
    (catch Exception ex
      ex)))

(defn- parse-graph-param-handler [required? inner-handler]
  (fn [request]
    (let [graph (get-in request [:params :graph])]
      (cond
        (some? graph)
        (let [uri-or-ex (try-parse-uri graph)]
          (if (instance? URI uri-or-ex)
            (inner-handler (assoc-in request [:params :graph] uri-or-ex))
            (unprocessable-entity-response "Valid URI required for graph parameter")))

        required?
        (unprocessable-entity-response "Graph parameter required")

        :else
        (inner-handler request)))))

(defn- require-graph-for-triples-rdf-format [inner-handler]
  (fn [{{:keys [rdf-format]} :params :as request}]
    (if (is-triples-format? rdf-format)
      (let [h (parse-graph-param-handler true inner-handler)]
        (h request))
      (inner-handler request))))

(defn- required-live-graph-param-handler [backend inner-handler]
  (fn [{{:keys [graph]} :params :as request}]
    (if (mgmt/is-graph-live? backend graph)
      (inner-handler request)
      (unprocessable-entity-response (str "Graph not found in live")))))

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

(defn- as-sync-write-job [f]
  (make-job
   :blocking-write [job]
    (let [result (f)]
      (ajobs/job-succeeded! job result))))

(defn- run-sync
  ([api-call-fn]
   (run-sync-job! (as-sync-write-job api-call-fn)))
  ([api-call-fn resp-fn]
   (run-sync-job! (as-sync-write-job api-call-fn)
                     resp-fn)))

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

(defn get-draftsets-handler
  ":get /draftsets"
  [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (wrap-authenticated
   (optional-enum-param
    :include #{:all :owned :claimable} :all
    (fn [{user :identity {:keys [include]} :params :as request}]
      (case include
        :all (ring/response (dsops/get-all-draftsets-info backend user))
        :claimable (ring/response (dsops/get-draftsets-claimable-by backend user))
        :owned (ring/response (dsops/get-draftsets-owned-by backend user)))))))

(defmethod ig/pre-init-spec ::get-draftsets-handler [_]
  (s/keys :req [:drafter/backend]))

(defmethod ig/init-key ::get-draftsets-handler [_ opts]
  (get-draftsets-handler opts))


(defn create-draftsets-handler [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (let [version "/v1"]
    (wrap-authenticated
     (fn [{{:keys [display-name description]} :params user :identity :as request}]
       (run-sync #(dsops/create-draftset! backend user display-name description)
                 (fn [result]
                   (if (jobutil/failed-job-result? result)
                     (response/api-response 500 result)
                     (ring/redirect-after-post (str version "/draftset/"
                                                    (get-in result [:details :id]))))))))))

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
  (log/info "creating wrap-as-draftset-owner")
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

(defn draftset-get-data [{wrap-as-draftset-owner :wrap-as-draftset-owner backend :drafter/backend
                          draftset-query-timeout-fn :timeout-fn}]
  (wrap-as-draftset-owner
   (parse-union-with-live-handler
    (fn [{{:keys [draftset-id graph union-with-live] :as params} :params :as request}]
      (let [executor (ep/draftset-endpoint {:backend backend :draftset-ref draftset-id :union-with-live? union-with-live})
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

(defmethod ig/pre-init-spec ::draftset-get-data [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner ::sp/timeout-fn]))

(defmethod ig/init-key ::draftset-get-data [_ opts]
  (draftset-get-data opts))

(defn delete-draftset-data-handler [{wrap-as-draftset-owner :wrap-as-draftset-owner backend :drafter/backend}]
  (wrap-as-draftset-owner
   (require-rdf-content-type
    (require-graph-for-triples-rdf-format
     (temp-file-body
      (fn [{{draftset-id :draftset-id
            graph :graph
            rdf-format :rdf-format} :params body :body :as request}]
        (let [delete-job (if (is-quads-format? rdf-format)
                           (dsjobs/delete-quads-from-draftset-job backend draftset-id body rdf-format)
                           (dsjobs/delete-triples-from-draftset-job backend draftset-id graph body rdf-format))]
          (submit-async-job! delete-job))))))))

(defmethod ig/pre-init-spec ::delete-draftset-data-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::delete-draftset-data-handler [_ opts]
  (delete-draftset-data-handler opts))

(defn delete-draftset-graph-handler [{backend :drafter/backend wrap-as-draftset-owner :wrap-as-draftset-owner}]
  (wrap-as-draftset-owner
   (parse-query-param-flag-handler
    :silent
    (parse-graph-param-handler
     true
     (fn [{{:keys [draftset-id graph silent]} :params :as request}]
       (if (mgmt/is-graph-managed? backend graph)
         (run-sync #(dsops/delete-draftset-graph! backend draftset-id graph)
                   #(draftset-sync-write-response % backend draftset-id))
         (if silent
           (ring/response (dsops/get-draftset-info backend draftset-id))
           (unprocessable-entity-response (str "Graph not found")))))))))

(defmethod ig/pre-init-spec ::delete-draftset-graph-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::delete-draftset-graph-handler [_ opts]
  (delete-draftset-graph-handler opts))

(defn delete-draftset-changes-handler [{backend :drafter/backend wrap-as-draftset-owner :wrap-as-draftset-owner}]
  (wrap-as-draftset-owner
   (parse-graph-param-handler
    true
    (fn [{{:keys [draftset-id graph]} :params}]
      (run-sync #(dsops/revert-graph-changes! backend draftset-id graph)
                (fn [result]
                  (if (jobutil/failed-job-result? result)
                    (response/api-response 500 result)
                    (if (= :reverted (:details result))
                      (ring/response (dsops/get-draftset-info backend draftset-id))
                      (ring/not-found "")))))))))

(defmethod ig/pre-init-spec ::delete-draftset-changes-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::delete-draftset-changes-handler [_ opts]
  (delete-draftset-changes-handler opts))

(defn put-draftset-data-handler [{backend :drafter/backend wrap-as-draftset-owner :wrap-as-draftset-owner}]
  (wrap-as-draftset-owner
   (require-rdf-content-type
    (require-graph-for-triples-rdf-format
     (temp-file-body
      (fn [{{draftset-id :draftset-id
            rdf-format :rdf-format
            graph :graph} :params body :body :as request}]
        (if (is-quads-format? rdf-format)
          (let [append-job (dsjobs/append-data-to-draftset-job backend draftset-id body rdf-format)]
            (submit-async-job! append-job))
          (let [append-job (dsjobs/append-triples-to-draftset-job backend draftset-id body rdf-format graph)]
            (submit-async-job! append-job)))))))))

(defmethod ig/pre-init-spec ::put-draftset-data-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::put-draftset-data-handler [_ opts]
  (put-draftset-data-handler opts))

(defn put-draftset-graph-handler [{backend :drafter/backend
                                   :keys [wrap-as-draftset-owner]}]
  (letfn [(required-live-graph-param [handler]
            (parse-graph-param-handler true (required-live-graph-param-handler backend handler)))]
    
    (wrap-as-draftset-owner
     (required-live-graph-param
      (fn [{{:keys [draftset-id graph]} :params}]
        (submit-async-job! (dsjobs/copy-live-graph-into-draftset-job backend draftset-id graph)))))))

(defmethod ig/pre-init-spec ::put-draftset-graph-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::put-draftset-graph-handler [_ opts]
  (put-draftset-graph-handler opts))

(defn draftset-query-handler [{backend :drafter/backend
                               :keys [wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (parse-union-with-live-handler
    (fn [{{:keys [draftset-id union-with-live]} :params :as request}]
      (let [;;executor (ep/draftset-endpoint {:backend backend :draftset-ref draftset-id :union-with-live? union-with-live})

            executor (backend/endpoint-repo backend draftset-id {:union-with-live? union-with-live})
            handler (sparql-protocol-handler {:repo executor :timeout-fn timeout-fn})]
        (handler request))))))

(defmethod ig/pre-init-spec ::draftset-query-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner ::sp/timeout-fn]))

(defmethod ig/init-key ::draftset-query-handler [_ opts]
  (draftset-query-handler opts))

(defn draftset-api-routes [{backend :drafter/backend
                            user-repo ::user/repo
                            authenticated :wrap-auth
                            draftset-query-timeout-fn :timeout-fn
                            :keys [get-draftsets-handler
                                   get-draftset-handler 
                                   create-draftsets-handler
                                   wrap-as-draftset-owner 
                                   delete-draftset-handler
                                   draftset-get-data-handler 
                                   delete-draftset-data-handler
                                   delete-draftset-graph-handler
                                   delete-draftset-changes-handler
                                   put-draftset-data-handler
                                   put-draftset-graph-handler
                                   draftset-query-handler]}]
  
  (let [version "/v1"]
    (context
     version []
     (routes
      (make-route :get "/users"
                  (authenticated
                   (fn [r]
                     (let [users (user/get-all-users user-repo)
                           summaries (map user/get-summary users)]
                       (ring/response summaries)))))

      (make-route :get "/draftsets" get-draftsets-handler)

      ;; create a new draftset
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

      (make-route :post "/draftset/:id/publish"
                  (wrap-as-draftset-owner
                   (fn [{{:keys [draftset-id]} :params user :identity}]
                     (if (user/has-role? user :publisher)
                       (submit-async-job! (dsjobs/publish-draftset-job backend draftset-id))
                       (forbidden-response "You require the publisher role to perform this action")))))

      (make-route :put "/draftset/:id"
                  (wrap-as-draftset-owner
                   (fn [{{:keys [draftset-id] :as params} :params}]
                     (run-sync #(dsops/set-draftset-metadata! backend draftset-id params)
                               #(draftset-sync-write-response % backend draftset-id)))))

      (make-route :post "/draftset/:id/submit-to"
                  (wrap-as-draftset-owner
                   (fn [{{:keys [user role draftset-id]} :params owner :identity}]
                     (cond
                       (and (some? user) (some? role))
                       (unprocessable-entity-response "Only one of user and role parameters permitted")

                       (some? user)
                       (if-let [target-user (user/find-user-by-username user-repo user)]
                         (run-sync #(dsops/submit-draftset-to-user! backend draftset-id owner target-user)
                                   #(draftset-sync-write-response % backend draftset-id))
                         (unprocessable-entity-response (str "User: " user " not found")))

                       (some? role)
                       (let [role-kw (keyword role)]
                         (if (user/is-known-role? role-kw)
                           (run-sync #(dsops/submit-draftset-to-role! backend draftset-id owner role-kw)
                                     #(draftset-sync-write-response % backend draftset-id))
                           (unprocessable-entity-response (str "Invalid role: " role))))

                       :else
                       (unprocessable-entity-response (str "user or role parameter required"))))))

      (make-route :put "/draftset/:id/claim"
                  (authenticated
                   (existing-draftset-handler
                    backend
                    (fn [{{:keys [draftset-id]} :params user :identity}]
                      (if-let [ds-info (dsops/get-draftset-info backend draftset-id)]
                        (if (user/can-claim? user ds-info)
                          (let [[result ds-info] (dsops/claim-draftset! backend draftset-id user)]
                            (if (= :ok result)
                              (ring/response ds-info)
                              (conflict-detected-response "Failed to claim draftset")))
                          (forbidden-response "User not in role for draftset claim"))
                        (ring/not-found "Draftset not found"))))))

      (make-route :put "/draftset/:id/claim"
                  (authenticated
                   (existing-draftset-handler
                    backend
                    (fn [{{:keys [draftset-id]} :params user :identity}]
                      (if-let [ds-info (dsops/get-draftset-info backend draftset-id)]
                        (if (user/can-claim? user ds-info)
                          (run-sync #(dsops/claim-draftset! backend draftset-id user)
                                    (fn [result]
                                      (if (jobutil/failed-job-result? result)
                                        (response/api-response 500 result)
                                        (let [[claim-outcome ds-info] (:details result)]
                                          (if (= :ok claim-outcome)
                                            (ring/response ds-info)
                                            (conflict-detected-response "Failed to claim draftset"))))))
                          (forbidden-response "User not in role for draftset claim"))
                        (ring/not-found "Draftset not found"))))))))))


(s/def ::wrap-auth fn?)

(defmethod ig/pre-init-spec :drafter.routes/draftsets-api [_]
  (s/keys :req-un [::wrap-auth ::sp/timeout-fn :drafter/backend
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
                   ::draftset-query-handler]
          :req [::user/repo]
          ;; TODO :req-un [::repo]
          ))

(defmethod ig/init-key :drafter.routes/draftsets-api [_ opts]
  (draftset-api-routes opts))
 
