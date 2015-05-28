(ns drafter.routes.drafts-api
  (:require [clojure.tools.logging :as log]
            [compojure.core :refer [DELETE POST PUT context routes]]
            [drafter.common.api-routes :as api-routes]
            [drafter.rdf.draft-management.jobs :refer [append-data-to-graph-from-file-job
                                                       create-draft-job
                                                       delete-graph-job
                                                       migrate-graph-live-job
                                                       is-failure-result?]]
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
        (api-routes/when-params [live-graph]
                                (submit-sync-job! (create-draft-job repo live-graph params)
                                                    (fn [result]
                                                      (if (is-failure-result? result)
                                                        (response/api-response 500 result)
                                                        (response/api-response 201 result))))))

      ;; deletes draft graph data contents; does not delete the draft graph
      ;; entry from the state graph.
      (DELETE "/contents" {{graph :graph} :params}
        (api-routes/when-params [graph]
                                (submit-async-job! (delete-graph-job repo graph :contents-only? true)))))

    ;; adds data to the graph from either source-graph or file
    ;; accepts extra meta- query string params, which are added to queue
    ;; metadata
    (routes
      (POST mount-point {{graph :graph} :params
                         {content-type :content-type} :params
                         query-params :query-params
                         {file :file} :params}
        (let [metadata (api-routes/meta-params query-params)]
          ;; when source graph not supplied: append from the file
          (api-routes/when-params [graph file]
                                  (submit-async-job!
                                    (append-data-to-graph-from-file-job repo graph
                                                                        (override-file-format content-type file)
                                                                        metadata))))))))

(defn graph-management-routes [mount-point repo]
  (routes
    ;; deletes draft graph data contents and then the draft graph itself
    (DELETE mount-point {{graph :graph} :params}
            (api-routes/when-params [graph]
                                    (submit-async-job! (delete-graph-job repo graph))))
   (context
     mount-point []
     ;; makes a graph live.
     (PUT "/live" {{graph :graph} :params
                   :as request}
          (log/info request)
          (api-routes/when-params [graph]
                                  (submit-async-job! (migrate-graph-live-job repo graph)))))))
