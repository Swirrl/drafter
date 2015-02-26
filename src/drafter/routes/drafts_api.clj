(ns drafter.routes.drafts-api
  (:require [clojure.tools.logging :as log]
            [compojure.core :refer [DELETE POST PUT context routes]]
            [drafter.common.api-routes :as api-routes]
            [drafter.rdf.draft-management.jobs :refer [append-data-to-graph-from-file-job
                                                       append-data-to-graph-from-graph-job
                                                       create-draft-job
                                                       delete-graph-job
                                                       migrate-graph-live-job
                                                       replace-data-from-graph-job
                                                       replace-graph-from-file-job]]
            [drafter.write-scheduler :refer [submit-job!]]))

(def no-file-or-graph-param-error-msg {:msg "You must supply both a 'file' and 'graph' parameter."})

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
    ;; accepts extra meta- query string params, which are added to the state graph

    (POST "/create" {{live-graph :live-graph} :params
                     params :params}
          (api-routes/when-params [live-graph]
                                  (submit-job! (create-draft-job repo live-graph params)
                                               {:job-desc (str "Creating draft for " live-graph)}))))

   ;; adds data to the graph from either source-graph or file
   ;; accepts extra meta- query string params, which are added to queue metadata
   (routes
    (POST mount-point {{graph :graph source-graph :source-graph} :params
                       {content-type :content-type} :params
                       query-params :query-params
                       {file :file} :params}

          (let [metadata (api-routes/meta-params query-params)]
            (if source-graph
              (api-routes/when-params [graph source-graph] ; when source supplied: append from source-graph.
                                      (submit-job!
                                       (append-data-to-graph-from-graph-job repo graph source-graph metadata)
                                       {:job-desc (str "append to graph: " graph " from source graph: " source-graph)
                                        :meta metadata}))

              (api-routes/when-params [graph file] ; when source graph not supplied: append from the file.
                                      (submit-job!
                                       (append-data-to-graph-from-file-job repo graph (override-file-format content-type file) metadata)
                                       {:job-desc (str "append to graph " graph " from file")
                                        :meta metadata})))))

    ;; replaces data in the graph from either source-graph or file
    ;; accepts extra meta- query string params, which are added to queue metadata

    (PUT mount-point {{graph :graph source-graph :source-graph} :params
                      {content-type :content-type} :params
                      query-params :query-params
                      {file :file} :params}
         (let [metadata (api-routes/meta-params query-params)]
           (if source-graph
             (api-routes/when-params [graph source-graph] ; when source supplied: replace from source-graph.
                                     (submit-job!
                                      (replace-data-from-graph-job repo graph source-graph metadata)
                                      {:job-desc (str "replace contents of graph " graph " from source graph: " source-graph)
                                       :meta metadata}))

             (api-routes/when-params [graph file] ; when source graph not supplied: replace from the file.
                                     (submit-job!
                                      (replace-graph-from-file-job repo graph (override-file-format content-type file) metadata)
                                      {:job-desc (str "replace contents of graph " graph " from file")
                                       :meta metadata}))))))))

(defn graph-management-routes [mount-point repo]
  (routes
   ;; deletes data in the graph. This could be a live or a draft graph.
   ;; accepts extra meta- query string params, which are added to queue metadata
   (DELETE mount-point {{graph :graph} :params
                        query-params :query-params}
           (api-routes/when-params [graph]
                                   (submit-job! (delete-graph-job repo graph)
                                                {:job-desc (str "delete graph" graph)
                                                 :meta (api-routes/meta-params query-params)})))
   (context
    mount-point []

    ;; makes a graph live.
    ;; accepts extra meta- query string params, which are added to queue metadata
    (PUT "/live" {{graph :graph} :params
                  query-params :query-params
                  :as request}
         (log/info request)
         (api-routes/when-params [graph]
                                 (submit-job! (migrate-graph-live-job repo graph)
                                              {:job-desc (str "migrate graph " graph " to live")
                                               :meta (api-routes/meta-params query-params)}))))))
