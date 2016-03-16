(ns drafter.routes.draftsets-api
  (:require [compojure.core :refer [ANY GET POST PUT DELETE context routes make-route]]
            [clojure.set :as set]
            [taoensso.timbre :as log]
            [ring.util.response :refer [redirect-after-post not-found response]]
            [drafter.responses :refer [unknown-rdf-content-type-response not-acceptable-response unprocessable-entity-response
                                       unsupported-media-type-response method-not-allowed-response forbidden-response submit-async-job!
                                       conflict-detected-response]]
            [drafter.requests :as request]
            [swirrl-server.responses :as response]
            [drafter.rdf.sparql-protocol :refer [process-sparql-query stream-sparql-response]]
            [drafter.backend.sesame.common.sparql-execution :refer [negotiate-graph-query-content-writer]]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.backend.protocols :refer :all]
            [drafter.util :as util]
            [drafter.user :as user]
            [drafter.user.repository :as user-repo]
            [drafter.middleware :refer [require-basic-authentication require-params allowed-methods-handler require-rdf-content-type temp-file-body]]
            [drafter.draftset :as ds]
            [grafter.rdf :refer [statements]]
            [drafter.rdf.sesame :refer [is-quads-format? is-triples-format?]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [clojure.java.io :as io])
  (:import [org.openrdf.query TupleQueryResultHandler]
           [org.openrdf OpenRDFException]
           [org.openrdf.queryrender RenderUtils]))

(defn- get-draftset-executor [backend draftset-ref]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)]
    (create-rewriter backend graph-mapping)))

(defn- execute-query-in-draftset [backend draftset-ref request union-with-live?]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)
        graph-restriction (mgmt/graph-mapping->graph-restriction backend graph-mapping union-with-live?)
        rewriting-executor (create-rewriter backend graph-mapping)]
    (process-sparql-query rewriting-executor request :graph-restrictions graph-restriction)))

(defn- get-accepted-rdf-format [request]
  (if-let [accept (request/accept request)]
    (try
      (mimetype->rdf-format accept)
      (catch Exception ex
        nil))))

(defn- get-draftset-data [backend draftset-ref accept-content-type union-with-live?]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)
        graph-restriction (mgmt/graph-mapping->graph-restriction backend graph-mapping union-with-live?)
        live->draft-graph-mapping (util/map-all util/string->sesame-uri graph-mapping)
        rewriting-executor (create-rewriter backend live->draft-graph-mapping)
        pquery (all-quads-query rewriting-executor graph-restriction)]
    (if-let [writer (negotiate-result-writer rewriting-executor pquery accept-content-type)]
      (let [exec-fn (create-query-executor rewriting-executor writer pquery)
            body (stream-sparql-response exec-fn drafter.operations/default-timeouts)]
        {:status 200
         :headers {"Content-Type" accept-content-type}
         :body body})
      (not-acceptable-response "Failed to negotiate output content format"))))

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
  (fn [request]
    (if-let [rdf-format (get-accepted-rdf-format request)]
      (inner-handler (assoc-in request [:params :rdf-format] rdf-format))
      (not-acceptable-response "Accept header required with MIME type of RDF format to return"))))

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

(defn draftset-api-routes [backend user-repo realm]
  (letfn [(authenticated [h] (require-basic-authentication user-repo realm h))
          (required-live-graph-param [h] (required-live-graph-param-handler backend h))
          (required-managed-graph-param [h] (required-managed-graph-param-handler backend h))
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
                     (fn [{user :identity :as request}]
                       (response (dsmgmt/get-all-draftsets-info backend user)))))

        (make-route :get "/draftsets/claimable"
                    (authenticated
                     (fn [{user :identity :as request}]
                       (response (dsmgmt/get-draftsets-claimable-by backend user)))))

        ;;create a new draftset
        (make-route :post "/draftsets"
                    (authenticated
                     (fn [{{:keys [display-name description]} :params user :identity :as request}]
                       (let [draftset-id (dsmgmt/create-draftset! backend user display-name description)]
                         (redirect-after-post (str version "/draftset/" draftset-id))))))

        (make-route :get "/draftset/:id"
                    (as-draftset-owner
                     (fn [{{:keys [draftset-id]} :params :as request}]
                       (if-let [info (dsmgmt/get-draftset-info backend draftset-id)]
                         (response info)
                         (not-found "")))))

        (make-route :delete "/draftset/:id"
                    (as-draftset-owner
                     (fn [{{:keys [draftset-id]} :params :as request}]
                       (delete-draftset! backend draftset-id)
                       (response ""))))

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
                      (require-graph-for-triples-rdf-format
                       (fn [{{:keys [draftset-id graph union-with-live rdf-format]} :params :as request}]
                         (if (is-quads-format? rdf-format)
                           (get-draftset-data backend draftset-id (request/accept request) (or union-with-live false))

                           ;; TODO fix this as it's vulnerable to SPARQL injection
                           (let [unsafe-query (format "CONSTRUCT {?s ?p ?o} WHERE { GRAPH <%s> { ?s ?p ?o } }" graph)
                                 escaped-query (RenderUtils/escape unsafe-query)
                                 query-request (assoc-in request [:params :query] escaped-query)]
                             (execute-query-in-draftset backend draftset-id query-request (or union-with-live false)))))))))

        (make-route :delete "/draftset/:id/data"
                    (as-draftset-owner
                     (require-rdf-content-type
                      (require-graph-for-triples-rdf-format
                       (temp-file-body
                        (fn [{{draftset-id :draftset-id
                               graph :graph
                               rdf-format :rdf-format} :params body :body :as request}]
                          (let [ds-executor (get-draftset-executor backend draftset-id)
                                delete-job (if (is-quads-format? rdf-format)
                                             (delete-quads-from-draftset-job ds-executor body rdf-format draftset-id)
                                             (delete-triples-from-draftset-job ds-executor body rdf-format draftset-id graph))]
                            (submit-async-job! delete-job))))))))

        (make-route :delete "/draftset/:id/graph"
                    (as-draftset-owner
                     (required-managed-graph-param
                      (fn [{{:keys [draftset-id graph]} :params}]
                        (dsmgmt/delete-draftset-graph! backend draftset-id graph)
                        (response "")))))

        (make-route :delete "/draftset/:id/changes"
                    (as-draftset-owner
                     (require-params #{:graph}
                                     (fn [{{:keys [draftset-id graph]} :params}]
                                       (let [result (dsmgmt/revert-graph-changes! backend draftset-id graph)]
                                         (if (= :reverted result)
                                           (response (dsmgmt/get-draftset-info backend draftset-id))
                                           (not-found "")))))))

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
                            (let [append-job (append-data-to-draftset-job backend draftset-id body rdf-format)]
                              (submit-async-job! append-job))
                            (let [append-job (append-triples-to-draftset-job backend draftset-id body rdf-format graph)]
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
                      (require-params #{:query}
                                      (fn [{{:keys [draftset-id query union-with-live]} :params :as request}]
                                        (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-id)
                                              uri-graph-mapping (util/map-all util/string->sesame-uri graph-mapping)
                                              rewriting-executor (create-rewriter backend uri-graph-mapping)
                                              graph-restriction (mgmt/graph-mapping->graph-restriction backend graph-mapping (or union-with-live false))]
                                          (process-sparql-query rewriting-executor request :graph-restrictions graph-restriction)))))))

        (make-route :post "/draftset/:id/publish"
                    (as-draftset-owner
                     (fn [{{:keys [draftset-id]} :params user :identity}]
                       (if (user/has-role? user :publisher)
                         (submit-async-job! (publish-draftset-job backend draftset-id))
                         (forbidden-response "You require the publisher role to perform this action")))))

        (make-route :put "/draftset/:id"
                    (as-draftset-owner
                     (fn [{{:keys [draftset-id] :as params} :params}]
                       (dsmgmt/set-draftset-metadata! backend draftset-id params)
                       (response (dsmgmt/get-draftset-info backend draftset-id)))))

        (make-route :post "/draftset/:id/submit"
                    (as-draftset-owner
                     (fn [{{:keys [draftset-id role]} :params user :identity}]
                       (let [role-kw (keyword role)]
                         (if (user/is-known-role? role-kw)
                           (do
                             (dsmgmt/submit-draftset! backend draftset-id user role-kw)
                             (response ""))
                           (unprocessable-entity-response (str "Invalid role: " role)))))))

        (make-route :put "/draftset/:id/claim"
                    (authenticated
                     (existing-draftset-handler
                      backend
                      (fn [{{:keys [draftset-id]} :params user :identity}]
                        (if-let [claim-role (dsmgmt/get-draftset-claim-role backend draftset-id)]
                          (if (user/has-role? user claim-role)
                            (let [[result ds-info] (dsmgmt/claim-draftset! backend draftset-id user)]
                              (if (= :ok result)
                                (response ds-info)
                                (conflict-detected-response "Failed to claim draftset")))
                            (forbidden-response "User not in role for draftset claim"))
                          (forbidden-response "Draftset has not been submitted"))))))

        (make-route :post "/draftset/:id/return"
                    (as-draftset-owner
                     (fn [{{:keys [draftset-id]} :params :as request}]
                       (dsmgmt/return-draftset! backend draftset-id)
                       (response "")))))))))
