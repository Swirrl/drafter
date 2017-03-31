(ns drafter.routes.draftsets-api
  (:require [clj-logging-config.log4j :as l4j]
            [compojure.core :as compojure :refer [context routes]]
            [drafter
             [draftset :as ds]
             [middleware :refer [allowed-methods-handler optional-enum-param require-params require-rdf-content-type temp-file-body]]
             [requests :as request]
             [responses :refer [conflict-detected-response forbidden-response not-acceptable-response run-sync-job! submit-async-job! unprocessable-entity-response]]
             [user :as user]
             [util :as util]]
            [drafter.rdf
             [content-negotiation :as conneg]
             [draft-management :as mgmt]
             [draftset-management :as dsmgmt]
             [sesame :refer [is-quads-format? is-triples-format?]]
             [sparql-protocol :refer [process-prepared-query process-sparql-query]]]
            [drafter.rdf.draft-management.jobs :refer [failed-job-result? make-job]]
            [drafter.user.repository :as user-repo]
            [ring.util.response :refer [not-found redirect-after-post response]]
            [swirrl-server.async.jobs :as ajobs]
            [swirrl-server.responses :as response])
  (:import org.openrdf.queryrender.RenderUtils))

(defn- existing-draftset-handler [backend inner-handler]
  (fn [{{:keys [id]} :params :as request}]
    (let [draftset-id (ds/->DraftsetId id)]
      (if (dsmgmt/draftset-exists? backend draftset-id)
        (let [updated-request (assoc-in request [:params :draftset-id] draftset-id)]
          (inner-handler updated-request))
        (not-found "")))))

(defn- restrict-to-draftset-owner [backend inner-handler]
  (fn [{user :identity {:keys [draftset-id]} :params :as request}]
    (if (dsmgmt/is-draftset-owner? backend draftset-id user)
      (inner-handler request)
      (forbidden-response "Operation only permitted by draftset owner"))))

(defn- rdf-response-format-handler [inner-handler]
  (fn [{{:keys [graph]} :params :as request}]
    (let [accept (request/accept request)]
      (if (some? graph)
        (if-let [[rdf-format _] (conneg/negotiate-rdf-triples-format accept)]
          (inner-handler (assoc-in request [:params :rdf-format] rdf-format))
          (not-acceptable-response "Accept header required with MIME type for RDF triples format to return"))
        (if-let [[rdf-format _] (conneg/negotiate-rdf-quads-format accept)]
          (inner-handler (assoc-in request [:params :rdf-format] rdf-format))
          (not-acceptable-response "Accept header required with MIME type for RDF quads format to return"))))))

(defn- require-graph-for-triples-rdf-format [inner-handler]
  (fn [{{:keys [graph rdf-format]} :params :as request}]
    (if (util/implies (is-triples-format? rdf-format) (some? graph))
      (inner-handler request)
      (unprocessable-entity-response "Graph parameter required for triples RDF format"))))

(defn- required-managed-graph-param-handler [backend inner-handler]
  (fn [{{:keys [graph]} :params :as request}]
    (if (mgmt/is-graph-managed? backend graph)
      (inner-handler request)
      (unprocessable-entity-response (str "Graph not found")))))

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
  (if (failed-job-result? result)
    (response/api-response 500 result)
    (response (dsmgmt/get-draftset-info backend draftset-id))))

(defn make-route [method route handler-fn]
  (compojure/make-route method route
                        (fn [req]
                          (l4j/with-logging-context
                            {:method method
                             :route route}
                            (handler-fn req)))))

(defn draftset-api-routes [backend user-repo authenticated]
  (letfn [(required-live-graph-param [h] (required-live-graph-param-handler backend h))
          (as-draftset-owner [h]
            (authenticated
             (existing-draftset-handler
              backend
              (restrict-to-draftset-owner backend h))))]
    (let [version "/v1"]
      (context
       version []
       (routes
        (make-route :get "/users"
                    (authenticated
                     (fn [r]
                       (let [users (user-repo/get-all-users user-repo)
                             summaries (map user/get-summary users)]
                         (response summaries)))))

        (make-route :get "/draftsets"
                    (authenticated
                     (optional-enum-param
                      :include #{:all :owned :claimable} :all
                      (fn [{user :identity {:keys [include]} :params :as request}]
                        (case include
                          :all (response (dsmgmt/get-all-draftsets-info backend user))
                          :claimable (response (dsmgmt/get-draftsets-claimable-by backend user))
                          :owned (response (dsmgmt/get-draftsets-owned-by backend user)))))))

        ;; create a new draftset
        (make-route :post "/draftsets"
                    (authenticated
                      (fn [{{:keys [display-name description]} :params user :identity :as request}]
                        (run-sync #(dsmgmt/create-draftset! backend user display-name description)
                                     (fn [result]
                                       (if (failed-job-result? result)
                                         (response/api-response 500 result)
                                         (redirect-after-post (str version
                                                                   "/draftset/"
                                                                   (get-in result [:details :id])))))))))

        (make-route :get "/draftset/:id"
                    (authenticated
                     (existing-draftset-handler
                      backend
                      (fn [{{:keys [draftset-id]} :params user :identity :as request}]
                        (if-let [info (dsmgmt/get-draftset-info backend draftset-id)]
                          (if (user/can-view? user info)
                            (response info)
                            (forbidden-response "Draftset not in accessible state"))
                          (not-found ""))))))

        (make-route :delete "/draftset/:id"
                    (as-draftset-owner
                     (fn [{{:keys [draftset-id]} :params :as request}]
                       (submit-async-job! (dsmgmt/delete-draftset-job backend draftset-id)))))

        (make-route :options "/draftset/:id"
                    (authenticated
                     (existing-draftset-handler
                      backend
                      (fn [{{:keys [draftset-id]} :params user :identity}]
                        (let [permitted (dsmgmt/find-permitted-draftset-operations backend draftset-id user)]
                          (response permitted))))))

        (make-route :get "/draftset/:id/data"
                    (as-draftset-owner
                     (rdf-response-format-handler
                      (parse-union-with-live-handler
                       (fn [{{:keys [draftset-id graph union-with-live rdf-format]} :params :as request}]
                         (if (is-quads-format? rdf-format)
                           (dsmgmt/get-draftset-data backend draftset-id (.getDefaultMIMEType rdf-format) union-with-live)
                           (let [unsafe-query (format "CONSTRUCT {?s ?p ?o} WHERE { GRAPH <%s> { ?s ?p ?o } }" graph)
                                 escaped-query (RenderUtils/escape unsafe-query)
                                 query-request (assoc-in request [:params :query] escaped-query)]
                             (dsmgmt/execute-query-in-draftset backend draftset-id query-request union-with-live))))))))

        (make-route :delete "/draftset/:id/data"
                    (as-draftset-owner
                     (require-rdf-content-type
                      (require-graph-for-triples-rdf-format
                       (temp-file-body
                        (fn [{{draftset-id :draftset-id
                              graph :graph
                              rdf-format :rdf-format} :params body :body :as request}]
                          (let [delete-job (if (is-quads-format? rdf-format)
                                             (dsmgmt/delete-quads-from-draftset-job backend draftset-id body rdf-format)
                                             (dsmgmt/delete-triples-from-draftset-job backend draftset-id graph body rdf-format))]
                            (submit-async-job! delete-job))))))))

        (make-route :delete "/draftset/:id/graph"
                    (as-draftset-owner
                      (parse-query-param-flag-handler
                        :silent
                        (fn [{{:keys [draftset-id graph silent] :as params} :params :as request}]
                          (if (mgmt/is-graph-managed? backend graph)
                            (run-sync #(dsmgmt/delete-draftset-graph! backend draftset-id graph)
                                         #(draftset-sync-write-response % backend draftset-id))
                            (if silent
                              (response (dsmgmt/get-draftset-info backend draftset-id))
                              (unprocessable-entity-response (str "Graph not found"))))))))

        (make-route :delete "/draftset/:id/changes"
                    (as-draftset-owner
                      (require-params #{:graph}
                                      (fn [{{:keys [draftset-id graph]} :params}]
                                        (run-sync #(dsmgmt/revert-graph-changes! backend draftset-id graph)
                                                     (fn [result]
                                                       (if (failed-job-result? result)
                                                         (response/api-response 500 result)
                                                         (if (= :reverted (:details result))
                                                           (response (dsmgmt/get-draftset-info backend draftset-id))
                                                           (not-found "")))))))))
        (make-route :put "/draftset/:id/data"
                    (as-draftset-owner
                     (require-rdf-content-type
                      (require-graph-for-triples-rdf-format
                       (temp-file-body
                        (fn [{{draftset-id :draftset-id
                               request-content-type :content-type
                               rdf-format :rdf-format
                               content-type :rdf-content-type
                               graph :graph} :params body :body :as request}]
                          (if (is-quads-format? rdf-format)
                            (let [append-job (dsmgmt/append-data-to-draftset-job backend draftset-id body rdf-format)]
                              (submit-async-job! append-job))
                            (let [append-job (dsmgmt/append-triples-to-draftset-job backend draftset-id body rdf-format graph)]
                              (submit-async-job! append-job)))))))))

        (make-route :put "/draftset/:id/graph"
                    (as-draftset-owner
                     (required-live-graph-param
                      (fn [{{:keys [draftset-id graph]} :params}]
                        (submit-async-job! (dsmgmt/copy-live-graph-into-draftset-job backend draftset-id graph))))))

        (make-route nil "/draftset/:id/query"
                    (allowed-methods-handler
                     #{:get :post}
                     (as-draftset-owner
                      (require-params
                       #{:query}
                       (parse-union-with-live-handler
                        (fn [{{:keys [draftset-id union-with-live]} :params :as request}]
                          (dsmgmt/execute-query-in-draftset backend draftset-id request union-with-live)))))))

        (make-route :post "/draftset/:id/publish"
                    (as-draftset-owner
                     (fn [{{:keys [draftset-id]} :params user :identity}]
                       (if (user/has-role? user :publisher)
                         (submit-async-job! (dsmgmt/publish-draftset-job backend draftset-id))
                         (forbidden-response "You require the publisher role to perform this action")))))

        (make-route :put "/draftset/:id"
                    (as-draftset-owner
                      (fn [{{:keys [draftset-id] :as params} :params}]
                        (run-sync #(dsmgmt/set-draftset-metadata! backend draftset-id params)
                                     #(draftset-sync-write-response % backend draftset-id)))))

        (make-route :post "/draftset/:id/submit-to"
                    (as-draftset-owner
                      (fn [{{:keys [user role draftset-id]} :params owner :identity}]
                        (cond
                          (and (some? user) (some? role))
                          (unprocessable-entity-response "Only one of user and role parameters permitted")

                          (some? user)
                          (if-let [target-user (user-repo/find-user-by-username user-repo user)]
                            (run-sync #(dsmgmt/submit-draftset-to-user! backend draftset-id owner target-user)
                                         #(draftset-sync-write-response % backend draftset-id))
                            (unprocessable-entity-response (str "User: " user " not found")))

                          (some? role)
                          (let [role-kw (keyword role)]
                            (if (user/is-known-role? role-kw)
                              (run-sync #(dsmgmt/submit-draftset-to-role! backend draftset-id owner role-kw)
                                           #(draftset-sync-write-response % backend draftset-id))
                              (unprocessable-entity-response (str "Invalid role: " role))))

                          :else
                          (unprocessable-entity-response (str "user or role parameter required"))))))

        (make-route :put "/draftset/:id/claim"
                    (authenticated
                      (existing-draftset-handler
                        backend
                        (fn [{{:keys [draftset-id]} :params user :identity}]
                          (if-let [ds-info (dsmgmt/get-draftset-info backend draftset-id)]
                            (if (user/can-claim? user ds-info)
                              (let [[result ds-info] (dsmgmt/claim-draftset! backend draftset-id user)]
                                (if (= :ok result)
                                  (response ds-info)
                                  (conflict-detected-response "Failed to claim draftset")))
                              (forbidden-response "User not in role for draftset claim"))
                            (not-found "Draftset not found"))))))

        (make-route :put "/draftset/:id/claim"
                    (authenticated
                      (existing-draftset-handler
                        backend
                        (fn [{{:keys [draftset-id]} :params user :identity}]
                          (if-let [ds-info (dsmgmt/get-draftset-info backend draftset-id)]
                            (if (user/can-claim? user ds-info)
                              (run-sync #(dsmgmt/claim-draftset! backend draftset-id user)
                                           (fn [result]
                                             (if (failed-job-result? result)
                                               (response/api-response 500 result)
                                               (let [[claim-outcome ds-info] (:details result)]
                                                 (if (= :ok claim-outcome)
                                                   (response ds-info)
                                                   (conflict-detected-response "Failed to claim draftset"))))))
                              (forbidden-response "User not in role for draftset claim"))
                            (not-found "Draftset not found")))))))))))
