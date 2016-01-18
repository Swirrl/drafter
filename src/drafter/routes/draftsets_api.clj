(ns drafter.routes.draftsets-api
  (:require [compojure.core :refer [ANY GET POST PUT DELETE context routes]]
            [clojure.set :as set]
            [ring.util.response :refer [redirect-after-post not-found response]]
            [drafter.responses :refer [unknown-rdf-content-type-response not-acceptable-response submit-async-job!]]
            [swirrl-server.responses :as response]
            [drafter.rdf.sparql-protocol :refer [process-sparql-query stream-sparql-response]]
            [drafter.backend.sesame.common.sparql-execution :refer [negotiate-graph-query-content-writer]]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.backend.protocols :refer :all]
            [drafter.util :as util]
            [grafter.rdf.io :refer [mimetype->rdf-format]])
  (:import [org.openrdf.query TupleQueryResultHandler]
           [org.openrdf.model.impl ContextStatementImpl URIImpl]))

(defn- is-quads-content-type? [rdf-format]
  (.supportsContexts rdf-format))

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

(defn draftset-api-routes [mount-point backend]
  (routes
   (context
    mount-point []

    (GET "/draftsets" []
         (response (dsmgmt/get-all-draftsets-info backend)))

    ;;create a new draftset
    (POST "/draftset" [display-name description]
          (if (some? display-name)
            (let [draftset-id (dsmgmt/create-draftset! backend display-name description)]
              (redirect-after-post (str mount-point "/draftset/" draftset-id)))
            (not-acceptable-response "display-name parameter required")))

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

    (POST "/draftset/:id/data" {{draftset-id :id
                        request-content-type :content-type
                        {file-part-content-type :content-type data :tempfile} :file} :params}
          (if-let [content-type (or file-part-content-type request-content-type)]
            (let [rdf-format (mimetype->rdf-format content-type)]
              (cond (nil? rdf-format)
                    (unknown-rdf-content-type-response content-type)
                    
                    (is-quads-content-type? rdf-format)
                    (let [append-job (append-data-to-draftset-job backend (dsmgmt/->DraftsetId draftset-id) data rdf-format)]
                      (submit-async-job! append-job))

                    :else (response/bad-request-response (str "Content type " content-type " does not map to an RDF format for quads"))))
            (response/bad-request-response "Content type required")))

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
              (not-found "")))))))
