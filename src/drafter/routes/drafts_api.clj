(ns drafter.routes.drafts-api
  (:require [clojure.tools.logging :as log]
            [drafter.util :refer [to-coll] :as util]
            [compojure.core :refer [GET DELETE POST PUT context routes]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [drafter.common.api-routes :as api-routes]
            [drafter.backend.protocols :refer :all]
            [drafter.rdf.draft-management :refer [drafter-state-graph create-draftset! get-draftset-info get-all-draftsets-info]]
            [drafter.rdf.draft-management.jobs :refer [failed-job-result?]]
            [drafter.responses :refer [submit-sync-job! submit-async-job! see-other-response]]
            [swirrl-server.responses :as response]))

(defn override-file-format
  "Takes a file object (hash) and if a non-nil file-format is supplied
  overrides its content-type."
  [file-format file-obj]
  (if file-format
    (assoc file-obj :content-type file-format)
    file-obj))

(defn- unknown-rdf-content-type-response [content-type]
  (response/bad-request-response
   (str "Unknown RDF format for content type " content-type)))

(defn- is-quads-content-type? [rdf-format]
  (.supportsContexts rdf-format))

(defn draftset-api-routes [mount-point backend]
  (routes
   (context
    mount-point []

    (GET "/all" []
         {:status 200 :headers {} :body (get-all-draftsets-info backend)})

    ;;create a new draftset
    (POST "/" [display-name description]
          (if (some? display-name)
            (let [draftset-id (create-draftset! backend display-name description)]
              (see-other-response (str mount-point "/" draftset-id)))
            {:status 406 :headers {} :body "dispaly-name parameter required"}))

    (GET "/:id" [id]
         (if-let [info (get-draftset-info backend (drafter.rdf.drafter-ontology/draftset-uri id))]
           {:status 200 :headers {} :body info}
           {:status 404 :headers {} :body ""}))

    (POST "/:id/data" {{draftset-id :id
                        request-content-type :content-type
                        {file-part-content-type :content-type data :tempfile} :file} :params}
          (if-let [content-type (or file-part-content-type request-content-type)]
            (let [rdf-format (mimetype->rdf-format content-type)
                  draftset-uri (drafter.rdf.drafter-ontology/draftset-uri draftset-id)]
              (cond (nil? rdf-format)
                    (unknown-rdf-content-type-response content-type)
                    
                    (is-quads-content-type? rdf-format)
                    (let [append-job (append-data-to-draftset-job backend draftset-uri data rdf-format)]
                      (submit-async-job! append-job))

                    :else (response/bad-request-response (str "Content type " content-type " does not map to an RDF format for quads"))))
            (response/bad-request-response "Content type required"))))))

(defn draft-api-routes [mount-point operations]
  (routes
    (context
      mount-point []

      ;; makes a new managed/draft graph.
      ;; accepts extra meta- query string params, which are added to the state
      ;; graph
      (POST "/create" {{live-graph :live-graph} :params
                       params :params}
            (response/when-params [live-graph]
                                  (if (= live-graph drafter-state-graph)
                                    (response/error-response 403 {:message "This graph is reserved, the create request was forbidden."})
                                    (submit-sync-job! (new-draft-job operations live-graph params)
                                                      (fn [result]
                                                        (if (failed-job-result? result)
                                                          (response/api-response 500 result)
                                                          (response/api-response 201 result)))))))

      (POST "/metadata" [graph :as {params :params}]
            (let [metadata (api-routes/meta-params params)
                  graphs (to-coll graph)]
              (if (or (empty? graph) (empty? metadata))
                {:status 400 :headers {} :body "At least one graph and metadata pair required"}
                (submit-sync-job! (update-metadata-job operations graphs metadata)))))

      (DELETE "/metadata" [graph meta-key]
              (let [meta-keys (to-coll meta-key)
                    graphs (to-coll graph)]
                (if (or (empty? graphs) (empty? meta-keys))
                  {:status 400 :headers {} :body "At least one graph and metadata key required"}
                  (submit-sync-job! (delete-metadata-job operations graphs meta-keys)))))

      ;; deletes draft graph data contents; does not delete the draft graph
      ;; entry from the state graph.
      (DELETE "/contents" {{graph :graph} :params}
        (response/when-params [graph]
                              (submit-async-job! (delete-graph-job operations graph true))))

      (POST "/copy-live" {{graph :graph} :params}
            (submit-async-job! (copy-from-live-graph-job operations graph))))

    ;; adds data to the graph from either source-graph or file
    ;; accepts extra meta- query string params, which are added to queue
    ;; metadata
    (routes
      (POST mount-point {{graph :graph} :params
                         {content-type :content-type} :params
                         params :params
                         {{file-part-content-type :content-type data :tempfile} :file} :params}
        (let [metadata (api-routes/meta-params params)
              data-content-type (or content-type file-part-content-type)]
          ;; when source graph not supplied: append from the file
          (response/when-params [graph data-content-type]
                                (if-let [rdf-format (mimetype->rdf-format data-content-type)]
                                  (submit-async-job!
                                   (append-data-to-graph-job operations graph
                                                             data rdf-format
                                                             metadata))
                                  (unknown-rdf-content-type-response data-content-type))))))))

(defn graph-management-routes [mount-point operations]
  (routes
    ;; deletes draft graph data contents and then the draft graph itself
    (DELETE mount-point {{graph :graph} :params}
            (response/when-params [graph]
                                  (submit-async-job! (delete-graph-job operations graph false))))
   (context
     mount-point []
     ;; makes a graph live.
     (PUT "/live" {{graph :graph} :params
                   :as request}
          (log/info request)
          (response/when-params [graph]
                                (let [graphs (to-coll graph)]
                                  (submit-async-job! (migrate-graphs-to-live-job operations graphs))))))))
