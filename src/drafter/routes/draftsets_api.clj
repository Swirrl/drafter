(ns drafter.routes.draftsets-api
  (:require [compojure.core :refer [ANY GET POST PUT DELETE context routes make-route]]
            [ring.util.response :refer [redirect-after-post not-found response]]
            [drafter.responses :refer [not-acceptable-response unprocessable-entity-response
                                       unsupported-media-type-response method-not-allowed-response
                                       forbidden-response submit-async-job! submit-sync-job!
                                       conflict-detected-response]]
            [drafter.requests :as request]
            [swirrl-server.responses :as response]
            [swirrl-server.async.jobs :refer [job-succeeded!]]
            [drafter.rdf.sparql-protocol :refer [sparql-protocol-handler sparql-execution-handler build-sparql-protocol-handler]]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.draft-management.jobs :refer [failed-job-result? make-job]]
            [drafter.rdf.content-negotiation :as conneg]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.endpoints :refer [draft-graph-set]]
            [drafter.util :as util]
            [drafter.user :as user]
            [drafter.user.repository :as user-repo]
            [drafter.middleware :refer [require-params allowed-methods-handler require-rdf-content-type
                                        temp-file-body optional-enum-param sparql-timeout-handler]]
            [drafter.draftset :as ds]
            [grafter.rdf :refer [statements]]
            [drafter.rdf.sesame :refer [is-quads-format? is-triples-format?]])
  (:import [org.openrdf.queryrender RenderUtils]))

(defn- get-draftset-executor [backend draftset-ref union-with-live?]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)]
    (draft-graph-set backend (util/map-all util/string->sesame-uri graph-mapping) union-with-live?)))

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

(defn- negotiate-sparql-results-content-type-with [negotiate-f format-type inner-handler]
  (fn [request]
    (let [accept (request/accept request)]
      (if-let [[rdf-format response-content-type] (negotiate-f accept)]
        (let [to-assoc {:format rdf-format
                        :response-content-type response-content-type}
              updated-request (update request :sparql #(merge % to-assoc))]
          (inner-handler updated-request))
        (not-acceptable-response (format "Accept header required with MIME type for RDF %s format to return" format-type))))))

(defn- require-graph-for-triples-rdf-format [inner-handler]
  (fn [{{:keys [graph rdf-format]} :params :as request}]
    (if (util/implies (is-triples-format? rdf-format) (some? graph))
      (inner-handler request)
      (unprocessable-entity-response "Graph parameter required for triples RDF format"))))

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
    :sync-write [job]
    (let [result (f)]
      (job-succeeded! job result))))

(defn- submit-sync
  ([api-call-fn]
   (submit-sync-job! (as-sync-write-job api-call-fn)))
  ([api-call-fn resp-fn]
   (submit-sync-job! (as-sync-write-job api-call-fn)
                     resp-fn)))

(defn- draftset-sync-write-response [result backend draftset-id]
  (if (failed-job-result? result)
    (response/api-response 500 result)
    (response (dsmgmt/get-draftset-info backend draftset-id))))

(defn draftset-api-routes [backend user-repo authenticated query-timeout]
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
                        (submit-sync #(dsmgmt/create-draftset! backend user display-name description)
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
                      (parse-union-with-live-handler
                        (fn [{{:keys [draftset-id graph union-with-live] :as params} :params :as request}]
                          (let [executor (get-draftset-executor backend draftset-id union-with-live)
                                conneg (if (contains? params :graph)
                                         #(negotiate-sparql-results-content-type-with conneg/negotiate-rdf-triples-format "triples" %)
                                         #(negotiate-sparql-results-content-type-with conneg/negotiate-rdf-quads-format "quads" %))
                                pquery (if (contains? params :graph)
                                         (let [unsafe-query (format "CONSTRUCT {?s ?p ?o} WHERE { GRAPH <%s> { ?s ?p ?o } }" graph)
                                               escaped-query (RenderUtils/escape unsafe-query)]
                                           (prepare-query executor escaped-query))
                                         (dsmgmt/all-quads-query executor))
                                prepare-handler (fn [inner-handler]
                                                  (fn [request]
                                                    (inner-handler (assoc-in request [:sparql :prepared-query] pquery))))
                                handler (->> sparql-execution-handler
                                             (sparql-timeout-handler query-timeout)
                                             (conneg)
                                             (prepare-handler))]
                            (handler request))))))

        (make-route :delete "/draftset/:id/data"
                    (as-draftset-owner
                     (require-rdf-content-type
                      (require-graph-for-triples-rdf-format
                       (temp-file-body
                        (fn [{{draftset-id :draftset-id
                               graph :graph
                               rdf-format :rdf-format} :params body :body :as request}]
                          (let [ds-executor (get-draftset-executor backend draftset-id false)
                                delete-job (if (is-quads-format? rdf-format)
                                             (dsmgmt/delete-quads-from-draftset-job ds-executor draftset-id body rdf-format)
                                             (dsmgmt/delete-triples-from-draftset-job ds-executor draftset-id graph body rdf-format))]
                            (submit-async-job! delete-job))))))))

        (make-route :delete "/draftset/:id/graph"
                    (as-draftset-owner
                      (parse-query-param-flag-handler
                        :silent
                        (fn [{{:keys [draftset-id graph silent]} :params :as request}]
                          (if (mgmt/is-graph-managed? backend graph)
                            (submit-sync #(dsmgmt/delete-draftset-graph! backend draftset-id graph)
                                         #(draftset-sync-write-response % backend draftset-id))
                            (if silent
                              (response (dsmgmt/get-draftset-info backend draftset-id))
                              (unprocessable-entity-response (str "Graph not found"))))))))

        (make-route :delete "/draftset/:id/changes"
                    (as-draftset-owner
                      (require-params #{:graph}
                                      (fn [{{:keys [draftset-id graph]} :params}]
                                        (submit-sync #(dsmgmt/revert-graph-changes! backend draftset-id graph)
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
                               rdf-format :rdf-format
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
                    (as-draftset-owner
                      (parse-union-with-live-handler
                        (fn [{{:keys [draftset-id union-with-live]} :params :as request}]
                          (let [executor (get-draftset-executor backend draftset-id union-with-live)
                                handler (sparql-protocol-handler executor query-timeout)]
                            (handler request))))))

        (make-route :post "/draftset/:id/publish"
                    (as-draftset-owner
                     (fn [{{:keys [draftset-id]} :params user :identity}]
                       (if (user/has-role? user :publisher)
                         (submit-async-job! (dsmgmt/publish-draftset-job backend draftset-id))
                         (forbidden-response "You require the publisher role to perform this action")))))

        (make-route :put "/draftset/:id"
                    (as-draftset-owner
                      (fn [{{:keys [draftset-id] :as params} :params}]
                        (submit-sync #(dsmgmt/set-draftset-metadata! backend draftset-id params)
                                     #(draftset-sync-write-response % backend draftset-id)))))

        (make-route :post "/draftset/:id/submit-to"
                    (as-draftset-owner
                      (fn [{{:keys [user role draftset-id]} :params owner :identity}]
                        (cond
                          (and (some? user) (some? role))
                          (unprocessable-entity-response "Only one of user and role parameters permitted")

                          (some? user)
                          (if-let [target-user (user-repo/find-user-by-username user-repo user)]
                            (submit-sync #(dsmgmt/submit-draftset-to-user! backend draftset-id owner target-user)
                                         #(draftset-sync-write-response % backend draftset-id))
                            (unprocessable-entity-response (str "User: " user " not found")))

                          (some? role)
                          (let [role-kw (keyword role)]
                            (if (user/is-known-role? role-kw)
                              (submit-sync #(dsmgmt/submit-draftset-to-role! backend draftset-id owner role-kw)
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
                              (submit-sync #(dsmgmt/claim-draftset! backend draftset-id user)
                                           (fn [result]
                                             (if (failed-job-result? result)
                                               (response/api-response 500 result)
                                               (let [[claim-outcome ds-info] (:details result)]
                                                 (if (= :ok claim-outcome)
                                                   (response ds-info)
                                                   (conflict-detected-response "Failed to claim draftset"))))))
                              (forbidden-response "User not in role for draftset claim"))
                            (not-found "Draftset not found")))))))))))