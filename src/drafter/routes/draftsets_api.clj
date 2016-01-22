(ns drafter.routes.draftsets-api
  (:require [compojure.core :refer [ANY GET POST PUT DELETE context routes]]
            [clojure.set :as set]
            [ring.util.response :refer [redirect-after-post not-found response]]
            [drafter.responses :refer [unknown-rdf-content-type-response not-acceptable-response unprocessable-entity-response
                                       unsupported-media-type-response submit-async-job!]]
            [swirrl-server.responses :as response]
            [drafter.rdf.sparql-protocol :refer [process-sparql-query stream-sparql-response]]
            [drafter.backend.sesame.common.sparql-execution :refer [negotiate-graph-query-content-writer]]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.backend.protocols :refer :all]
            [drafter.util :as util]
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

(defn- execute-query-in-draftset [backend draftset-ref request]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)
        rewriting-executor (create-rewriter backend graph-mapping)]
    (process-sparql-query rewriting-executor request :graph-restrictions (vals graph-mapping))))

(defn- get-accepted-rdf-format [request]
  (if-let [accept (get-in request [:headers "Accept"])]
    (try
      (mimetype->rdf-format accept)
      (catch Exception ex
        nil))))

(defn- get-draftset-data [backend draftset-ref accept-content-type]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)
        live->draft-graph-mapping (util/map-all util/string->sesame-uri graph-mapping)
        rewriting-executor (create-rewriter backend live->draft-graph-mapping)
        pquery (all-quads-query rewriting-executor (vals graph-mapping))]
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

(defn draftset-api-routes [mount-point backend]
  (routes
   (context
    mount-point []

    (GET "/draftsets" []
         (response (dsmgmt/get-all-draftsets-info backend)))

    ;;create a new draftset
    (POST "/draftset" [display-name description]
          (let [draftset-id (dsmgmt/create-draftset! backend display-name description)]
            (redirect-after-post (str mount-point "/draftset/" draftset-id))))

    (GET "/draftset/:id" [id]
         (if-let [info (dsmgmt/get-draftset-info backend (dsmgmt/->DraftsetId id))]
           (response info)
           (not-found "")))

    (DELETE "/draftset/:id" [id]
            (delete-draftset! backend (dsmgmt/->DraftsetId id))
            (response ""))

    (GET "/draftset/:id/data" [id graph union-with-live :as request]
         (let [ds-id (dsmgmt/->DraftsetId id)]
           (if (dsmgmt/draftset-exists? backend ds-id)
             (if-let [rdf-format (get-accepted-rdf-format request)]
               (if (is-quads-content-type? rdf-format)
                 (get-draftset-data backend ds-id (get-in request [:headers "Accept"]))
                 (if (some? graph)
                   (let [q (format "CONSTRUCT {?s ?p ?o} WHERE { GRAPH <%s> { ?s ?p ?o } }" graph)
                         query-request (assoc-in request [:params :query] q)]
                     (execute-query-in-draftset backend ds-id query-request))
                   (not-acceptable-response "graph query parameter required for RDF triple format")))
               (not-acceptable-response "Accept header required with MIME type of RDF format to return"))
             (not-found ""))))

    (DELETE "/draftset/:id/data" {{draftset-id :id
                                   request-content-type :content-type
                                   graph :graph
                                   {file-part-content-type :content-type data :tempfile} :file} :params :as request}
            (let [ds-id (dsmgmt/->DraftsetId draftset-id)
                  content-type (or file-part-content-type request-content-type)]
              (if (dsmgmt/draftset-exists? backend ds-id)
                (if-let [rdf-format (mimetype->rdf-format content-type)]
                  (let [ds-executor (get-draftset-executor backend ds-id)]
                    (if (implies (is-triples-rdf-format? rdf-format)
                                 (some? graph))
                      (try
                        (let [statements-to-delete (read-statements data rdf-format)]
                          (if (is-quads-content-type? rdf-format)
                            (delete-quads ds-executor statements-to-delete nil)
                            (delete-triples ds-executor statements-to-delete (URIImpl. graph)))
                          (response (dsmgmt/get-draftset-info backend ds-id)))
                        (catch OpenRDFException ex
                          (unprocessable-entity-response "Cannot read statements to delete")))
                      (not-acceptable-response "graph parameter required for triples RDF format")))
                  (unsupported-media-type-response (str "Unsupported media type: " (or content-type ""))))
                (not-found ""))))

    (DELETE "/draftset/:id/graph" [id graph]
            (let [ds-id (dsmgmt/->DraftsetId id)]
              (if (dsmgmt/draftset-exists? backend ds-id)
                (do
                  (dsmgmt/delete-draftset-graph! backend ds-id graph)
                  (response {}))
                (not-found ""))))

    (POST "/draftset/:id/data" {{draftset-id :id
                        request-content-type :content-type
                        {file-part-content-type :content-type data :tempfile} :file} :params}
          (let [ds-id (dsmgmt/->DraftsetId draftset-id)]
            (if (dsmgmt/draftset-exists? backend ds-id)
              (if-let [content-type (or file-part-content-type request-content-type)]
                (let [rdf-format (mimetype->rdf-format content-type)]
                  (cond (nil? rdf-format)
                        (unknown-rdf-content-type-response content-type)
                    
                        (is-quads-content-type? rdf-format)
                        (let [append-job (append-data-to-draftset-job backend (dsmgmt/->DraftsetId draftset-id) data rdf-format)]
                          (submit-async-job! append-job))

                        :else (response/bad-request-response (str "Content type " content-type " does not map to an RDF format for quads"))))
                (response/bad-request-response "Content type required"))
              (not-found ""))))

    (ANY "/draftset/:id/query" [id query :as request]
         (cond (not (#{:get :post} (:request-method request)))
               (not-found "")

               (nil? query) (not-acceptable-response "query parameter required")

               :else
               (let [id (dsmgmt/->DraftsetId id)]
               (if (dsmgmt/draftset-exists? backend id)
                 (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend id)
                       rewriting-executor (create-rewriter backend graph-mapping)]
                   (process-sparql-query rewriting-executor request :graph-restrictions (vals graph-mapping)))
                 (not-found "")))))

    (POST "/draftset/:id/publish" [id]
          (let [id (dsmgmt/->DraftsetId id)]
            (if (dsmgmt/draftset-exists? backend id)
              (submit-async-job! (publish-draftset-job backend id))
              (not-found ""))))

    (PUT "/draftset/:id/meta" [id :as request]
         (let [ds-id (dsmgmt/->DraftsetId id)]
           (if (dsmgmt/draftset-exists? backend ds-id)
             (do
               (dsmgmt/set-draftset-metadata! backend ds-id (:params request))
               (response (dsmgmt/get-draftset-info backend ds-id)))
             (not-found "")))))))
