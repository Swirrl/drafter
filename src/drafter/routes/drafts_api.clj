(ns drafter.routes.drafts-api
  (:require [clojure.tools.logging :as log]
            [compojure.core :refer [DELETE POST PUT context routes]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [drafter.common.api-routes :as api-routes]
            [drafter.rdf.draft-management.jobs :refer [append-data-to-graph-from-file-job
                                                       create-draft-job
                                                       delete-graph-job
                                                       migrate-graph-live-job
                                                       create-update-metadata-job
                                                       failed-job-result?]]
            [drafter.responses :refer [submit-sync-job! submit-async-job!]]
            [swirrl-server.responses :as response]))

(defn override-file-format
  "Takes a file object (hash) and if a non-nil file-format is supplied
  overrides its content-type."
  [file-format file-obj]
  (if file-format
    (assoc file-obj :content-type file-format)
    file-obj))

(defn draft-api-routes [mount-point repo]
  (routes
    (context
      mount-point []

      ;; makes a new managed/draft graph.
      ;; accepts extra meta- query string params, which are added to the state
      ;; graph
      (POST "/create" {{live-graph :live-graph} :params
                       params :params}
        (response/when-params [live-graph]
                              (submit-sync-job! (create-draft-job repo live-graph params)
                                                (fn [result]
                                                  (if (failed-job-result? result)
                                                    (response/api-response 500 result)
                                                    (response/api-response 201 result))))))

      (POST "/metadata" [graph :as {params :query-params}]
            (let [metadata (api-routes/meta-params params)
                  graphs (if (coll? graph) graph [graph])]
              (if (or (empty? graph) (empty? metadata))
                {:status 400 :headers {} :body "At least one graph and metadata pair required"}
                (submit-sync-job! (create-update-metadata-job repo graphs metadata)))))

      ;; deletes draft graph data contents; does not delete the draft graph
      ;; entry from the state graph.
      (DELETE "/contents" {{graph :graph} :params}
        (response/when-params [graph]
                              (submit-async-job! (delete-graph-job repo graph :contents-only? true)))))

    ;; adds data to the graph from either source-graph or file
    ;; accepts extra meta- query string params, which are added to queue
    ;; metadata
    (routes
      (POST mount-point {{graph :graph} :params
                         {content-type :content-type} :params
                         query-params :query-params
                         {{file-part-content-type :content-type data :tempfile} :file} :params}
        (let [metadata (api-routes/meta-params query-params)
              data-content-type (or content-type file-part-content-type)]
          ;; when source graph not supplied: append from the file
          (response/when-params [graph data-content-type]
                                (if-let [rdf-format (mimetype->rdf-format data-content-type)]
                                  (submit-async-job!
                                   (append-data-to-graph-from-file-job repo graph
                                                                       data rdf-format
                                                                       metadata))
                                  (response/bad-request-response (str "Unknown RDF format for content type " data-content-type)))))))))

(defn graph-management-routes [mount-point repo]
  (routes
    ;; deletes draft graph data contents and then the draft graph itself
    (DELETE mount-point {{graph :graph} :params}
            (response/when-params [graph]
                                  (submit-async-job! (delete-graph-job repo graph))))
   (context
     mount-point []
     ;; makes a graph live.
     (PUT "/live" {{graph :graph} :params
                   :as request}
          (log/info request)
          (response/when-params [graph]
                                (submit-async-job! (migrate-graph-live-job repo graph)))))))
