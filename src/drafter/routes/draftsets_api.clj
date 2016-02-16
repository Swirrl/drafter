(ns drafter.routes.draftsets-api
  (:require [compojure.core :refer [ANY GET POST PUT DELETE context routes make-route]]
            [clojure.set :as set]
            [ring.util.response :refer [redirect-after-post not-found response]]
            [drafter.responses :refer [unknown-rdf-content-type-response not-acceptable-response unprocessable-entity-response
                                       unsupported-media-type-response method-not-allowed-response forbidden-response submit-async-job!]]
            [swirrl-server.responses :as response]
            [drafter.rdf.sparql-protocol :refer [process-sparql-query stream-sparql-response]]
            [drafter.backend.sesame.common.sparql-execution :refer [negotiate-graph-query-content-writer]]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.backend.protocols :refer :all]
            [drafter.util :as util]
            [drafter.user :as user]
            [drafter.draftset :as ds]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.io :refer [mimetype->rdf-format]])
  (:import [org.openrdf.query TupleQueryResultHandler]
           [org.openrdf OpenRDFException]
           [org.openrdf.rio Rio]
           [org.openrdf.rio.helpers StatementCollector]
           [org.openrdf.model.impl URIImpl]))

(defn- is-quads-content-type? [rdf-format]
  (.supportsContexts rdf-format))

(defn- is-triples-rdf-format? [rdf-format]
  (not (is-quads-content-type? rdf-format)))

(defn- implies [p q]
  (or (not p) q))

(defn- get-draftset-executor [backend draftset-ref]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)
        live->draft-graph-mapping (util/map-all util/string->sesame-uri graph-mapping)]
    (create-rewriter backend live->draft-graph-mapping)))

(defn- execute-query-in-draftset [backend draftset-ref request union-with-live?]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)
        graph-restriction (mgmt/graph-mapping->graph-restriction backend graph-mapping union-with-live?)
        rewriting-executor (create-rewriter backend graph-mapping)]
    (process-sparql-query rewriting-executor request :graph-restrictions graph-restriction)))

(defn- get-accepted-rdf-format [request]
  (if-let [accept (get-in request [:headers "Accept"])]
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

(defn- read-statements [in-stream format]
  (let [parser (Rio/createParser format)
        model (java.util.ArrayList.)
        base-uri ""]
    (.setRDFHandler parser (StatementCollector. model))
    (.parse parser in-stream base-uri)
    (seq model)))

(defn- allowed-methods-handler [is-allowed-fn inner-handler]
  (fn [{:keys [request-method] :as request}]
    (if (is-allowed-fn request-method)
      (inner-handler request)
      (method-not-allowed-response request-method))))

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

(defn- rdf-file-part-handler [inner-handler]
  (fn [{{request-content-type :content-type
         {file-part-content-type :content-type data :tempfile} :file} :params :as request}]
    (if-let [content-type (or file-part-content-type request-content-type)]
      (if-let [rdf-format (mimetype->rdf-format content-type)]
        (let [modified-request (update-in request [:params] #(merge % {:rdf-format rdf-format
                                                                       :rdf-content-type content-type}))]
          (inner-handler modified-request))
        (unsupported-media-type-response (str "Unsupported media type: " (or content-type ""))))
      (response/bad-request-response "Content type required"))))

(defn- rdf-response-format-handler [inner-handler]
  (fn [request]
    (if-let [rdf-format (get-accepted-rdf-format request)]
      (inner-handler (assoc-in request [:params :rdf-format] rdf-format))
      (not-acceptable-response "Accept header required with MIME type of RDF format to return"))))

(defn- read-rdf-file-handler
  "NOTE: This middleware must come after rdf-file-part-handler since
  that ensure the incoming request is well-formed and has a known
  content type."
  [inner-handler]
  (fn [{{rdf-format :rdf-format
         {file-part-content-type :content-type data :tempfile} :file} :params :as request}]
    (try
      (let [rdf-statements (read-statements data rdf-format)
            modified-request (assoc-in request [:params :rdf-statements] rdf-statements)]
        (inner-handler modified-request))
      (catch OpenRDFException ex
        (unprocessable-entity-response "Cannot read statements to delete")))))

(defn draftset-api-routes [backend]
  (routes

   (GET "/draftsets" {user :identity :as request}
        (response (dsmgmt/get-all-draftsets-info backend user)))

   (GET "/draftsets/offered" {user :identity :as request}
        (response (dsmgmt/get-draftsets-offered-to backend user)))

    ;;create a new draftset

   (POST "/draftset" {{:keys [display-name description]} :params user :identity :as request}
         (let [draftset-id (dsmgmt/create-draftset! backend user display-name description)]
           (redirect-after-post (str "/draftset/" draftset-id))))

   (make-route :get "/draftset/:id"
               (existing-draftset-handler
                backend
                (restrict-to-draftset-owner
                 backend
                 (fn [{{:keys [draftset-id]} :params :as request}]
                   (if-let [info (dsmgmt/get-draftset-info backend draftset-id)]
                     (response info)
                     (not-found ""))))))

   (make-route :delete "/draftset/:id"
               (existing-draftset-handler
                backend
                (restrict-to-draftset-owner
                 backend
                 (fn [{{:keys [draftset-id]} :params :as request}]
                   (delete-draftset! backend draftset-id)
                   (response "")))))

   (make-route :get "/draftset/:id/data"
               (existing-draftset-handler
                backend
                (rdf-response-format-handler
                 (fn [{{:keys [draftset-id graph union-with-live rdf-format]} :params :as request}]
                   (if (is-quads-content-type? rdf-format)
                     (get-draftset-data backend draftset-id (get-in request [:headers "Accept"]) (or union-with-live false))
                     (if (some? graph)
                       (let [q (format "CONSTRUCT {?s ?p ?o} WHERE { GRAPH <%s> { ?s ?p ?o } }" graph)
                             query-request (assoc-in request [:params :query] q)]
                         (execute-query-in-draftset backend draftset-id query-request (or union-with-live false)))
                       (not-acceptable-response "graph query parameter required for RDF triple format")))))))

   (make-route :delete "/draftset/:id/data"
               (existing-draftset-handler
                backend
                (restrict-to-draftset-owner
                 backend
                 (rdf-file-part-handler
                  (read-rdf-file-handler
                   (fn [{{draftset-id :draftset-id
                          graph :graph
                          rdf-format :rdf-format
                          statements-to-delete :rdf-statements} :params :as request}]
                     (let [ds-executor (get-draftset-executor backend draftset-id)]
                       (if (implies (is-triples-rdf-format? rdf-format)
                                    (some? graph))

                         (let [delete-job (if (is-quads-content-type? rdf-format)
                                            (delete-quads-from-draftset-job ds-executor statements-to-delete draftset-id)
                                            (delete-triples-from-draftset-job ds-executor statements-to-delete draftset-id (util/string->sesame-uri graph)))]
                           (submit-async-job! delete-job))
                         (not-acceptable-response "graph parameter required for triples RDF format")))))))))

   (make-route :delete "/draftset/:id/graph"
               (existing-draftset-handler
                backend
                (restrict-to-draftset-owner
                 backend
                 (fn [{{:keys [draftset-id graph]} :params}]
                   (dsmgmt/delete-draftset-graph! backend draftset-id graph)
                   (response {})))))

   (make-route :post "/draftset/:id/data"
               (existing-draftset-handler
                backend
                (restrict-to-draftset-owner
                 backend
                 (rdf-file-part-handler
                  (fn [{{draftset-id :draftset-id
                         request-content-type :content-type
                         rdf-format :rdf-format
                         content-type :rdf-content-type
                         graph :graph
                         {data :tempfile} :file} :params}]
                    (if (is-quads-content-type? rdf-format)
                      (let [append-job (append-data-to-draftset-job backend draftset-id data rdf-format)]
                        (submit-async-job! append-job))
                      (if (some? graph)
                        (let [append-job (append-triples-to-draftset-job backend draftset-id data rdf-format graph)]
                          (submit-async-job! append-job))
                        (response/bad-request-response "Graph required for triples content type"))))))))

   (make-route nil "/draftset/:id/query"
               (allowed-methods-handler
                #{:get :post}
                (existing-draftset-handler
                 backend
                 (restrict-to-draftset-owner
                  backend
                  (fn [{{:keys [draftset-id query union-with-live]} :params :as request}]
                    (if (nil? query)
                      (not-acceptable-response "query parameter required")
                      (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-id)
                            uri-graph-mapping (util/map-all util/string->sesame-uri graph-mapping)
                            rewriting-executor (create-rewriter backend uri-graph-mapping)
                            graph-restriction (mgmt/graph-mapping->graph-restriction backend graph-mapping (or union-with-live false))]
                        (process-sparql-query rewriting-executor request :graph-restrictions graph-restriction))))))))

   (make-route :post "/draftset/:id/publish"
               (existing-draftset-handler
                backend
                (restrict-to-draftset-owner
                 backend
                 (fn [{{:keys [draftset-id]} :params user :identity}]
                   (if (user/has-role? user :publisher)
                     (submit-async-job! (publish-draftset-job backend draftset-id))
                     (forbidden-response "You require the publisher role to perform this action"))))))

   (make-route :put "/draftset/:id/meta"
               (existing-draftset-handler
                backend
                (restrict-to-draftset-owner
                 backend
                 (fn [{{:keys [draftset-id] :as params} :params}]
                   (dsmgmt/set-draftset-metadata! backend draftset-id params)
                   (response (dsmgmt/get-draftset-info backend draftset-id))))))

   (make-route :post "/draftset/:id/offer"
               (existing-draftset-handler
                backend
                (restrict-to-draftset-owner
                 backend
                 (fn [{{:keys [draftset-id role]} :params user :identity}]
                   (let [role-kw (keyword role)]
                     (if (user/is-known-role? role-kw)
                       (do
                         (dsmgmt/offer-draftset! backend draftset-id user role-kw)
                         (response ""))
                       (swirrl-server.responses/bad-request-response (str "Invalid role: " role))))))))

   (make-route :post "/draftset/:id/claim"
               (existing-draftset-handler
                backend
                (fn [{{:keys [draftset-id]} :params user :identity}]
                  (let [[result ds-info] (dsmgmt/claim-draftset! backend draftset-id user)]
                    (case result
                      :ok (response ds-info)
                      :role (forbidden-response "User not in role for draftset offer")
                      :owned (forbidden-response "Draftset not on offer")
                      (forbidden-response "Failed to claim draftset"))))))

   (make-route :post "/draftset/:id/return"
               (existing-draftset-handler
                backend
                (restrict-to-draftset-owner
                 backend
                 (fn [{{:keys [draftset-id]} :params :as request}]
                   (dsmgmt/return-draftset! backend draftset-id)
                   (response "")))))))
