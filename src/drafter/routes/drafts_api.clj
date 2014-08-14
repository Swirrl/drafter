(ns drafter.routes.drafts-api
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [ring.util.io :as io]
            [ring.middleware.multipart-params]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]]
            [drafter.rdf.draft-management :as mgmt]
            [taoensso.timbre :as timbre]
            [drafter.rdf.queue :as q]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point process-sparql-query]]
            [drafter.common.api-routes :as api-routes]
            [grafter.rdf.sesame :as ses]
            [grafter.rdf.protocols :refer [add statements]])
  (:import [org.openrdf.query.resultio TupleQueryResultFormat BooleanQueryResultFormat]))

(defn enqueue-job!
  "Enqueues a job function on the queue and returns a ring HTTP
  response."

  [queue job-function opts]
  (let []
    (if-let [queue-id (q/offer! queue job-function opts)]
      (api-routes/api-response 202 {:queue-id queue-id :msg "Your request was accepted"})
      (api-routes/error-response 503 {:msg "The queue is temporarily full.  Please try again later."}))))

(def no-file-or-graph-param-error-msg {:msg "You must supply both a 'file' and 'graph' parameter."})

(defn replace-graph-from-file-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified file."
  [repo graph {:keys [tempfile size filename] :as file}]
  (fn []
    (let [format (ses/filename->rdf-format filename)]
      (timbre/info (str "Replacing graph " graph " with contents of file " tempfile "[" filename " " size " bytes]"))
      (ses/with-transaction repo
        (mgmt/replace-data! repo graph (statements tempfile :format format)))
      (timbre/info (str "Replaced graph " graph " with file " tempfile "[" filename "]")))))

(defn append-data-to-graph-from-file-job
  "Return a job function that adds the triples from the specified file
  to the specified graph."
  [repo graph {:keys [tempfile size filename] :as file}]
  (fn []
    (let [format (ses/filename->rdf-format filename)]
      (timbre/info (str "Appending contents of file " tempfile "[" filename " " size " bytes] to graph: " graph))

      (ses/with-transaction repo
        (mgmt/append-data! repo graph (statements (:tempfile file) :format format)))

      (timbre/info (str "File import (append) complete " tempfile " to graph: " graph)))))

(defn append-data-to-graph-from-graph-job
  "Return a job function that adds the triples from the specified named graph to the specified graph"
  [repo graph source-graph]
  (fn []
    (timbre/info (str "Appending contents of " source-graph "  to graph: " graph))

    (let [query-str (str "CONSTRUCT { ?s ?p ?o } WHERE
                         { GRAPH <" source-graph "> { ?s ?p ?o } }")
          source-data (ses/query repo query-str)]

      (ses/with-transaction repo
        (mgmt/append-data! repo graph source-data))

      (timbre/info (str "Graph import complete. Imported contents of " source-graph " to graph: " graph)))))

(defn replace-data-from-graph-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified source graph."
  [repo graph source-graph]
  (fn []
    (timbre/info (str "Replacing graph " graph " with contents of graph: " source-graph ))

    (let [query-str (str "CONSTRUCT { ?s ?p ?o } WHERE
                         { GRAPH <" source-graph "> { ?s ?p ?o } }")
          source-data (ses/query repo query-str)]
          (ses/with-transaction repo
            (mgmt/replace-data! repo graph source-data))
          (timbre/info (str "Graph replace complete. Replaced contents of " source-graph " into graph: " graph)))))

(defn delete-graph-job [repo graph]
  (fn []
    (ses/with-transaction repo
      (mgmt/delete-graph-and-draft-state! repo graph))))

(defn migrate-graph-live-job [repo graph]
  (fn []
    (ses/with-transaction repo
      (mgmt/migrate-live! repo graph))))

(defn draft-api-routes [mount-point repo queue]
  (routes
   (context
    mount-point []

    ;; makes a new managed/draft graph.
    ;; accepts extra meta- query string params, which are added to the state graph

    (POST "/create" {{live-graph "live-graph"} :query-params
                     query-params :query-params}

          (api-routes/when-params [live-graph]
                                  (let [draft-graph-uri (ses/with-transaction repo
                                                          (mgmt/create-managed-graph! repo live-graph)
                                                          (mgmt/create-draft-graph! repo live-graph (api-routes/meta-params query-params)))]
                                    (api-routes/api-response 201 {:guri draft-graph-uri})))))

   ;; adds data to the graph from either source-graph or file
   ;; accepts extra meta- query string params, which are added to queue metadata
   (routes
    (POST mount-point {{graph "graph" source-graph "source-graph"} :query-params
                       query-params :query-params
                       {file :file} :params}


          (if source-graph
            (api-routes/when-params [graph source-graph] ; when source supplied: append from source-graph.
                                    (enqueue-job! queue
                                                  (append-data-to-graph-from-graph-job repo graph source-graph)
                                                  {:job-desc (str "append to graph: " graph " from source graph: " source-graph)
                                                   :meta (api-routes/meta-params query-params)}))

            (api-routes/when-params [graph file] ; when source graph not supplied: append from the file.
                                    (enqueue-job! queue
                                                  (append-data-to-graph-from-file-job repo graph file)
                                                  {:job-desc (str "append to graph " graph " from file")
                                                   :meta (api-routes/meta-params query-params)}))))

    ;; replaces data in the graph from either source-graph or file
    ;; accepts extra meta- query string params, which are added to queue metadata
    (PUT mount-point {{graph "graph" source-graph "source-graph"} :query-params
                      query-params :query-params
                      {file :file} :params}

         (if source-graph
           (api-routes/when-params [graph source-graph] ; when source supplied: replace from source-graph.
                                   (enqueue-job! queue
                                                 (replace-data-from-graph-job repo graph source-graph)
                                                 {:job-desc (str "replace contents of graph " graph " from source graph: " source-graph)
                                                  :meta (api-routes/meta-params query-params)}))

           (api-routes/when-params [graph file] ; when source graph not supplied: replace from the file.
                                   (enqueue-job! queue
                                                 (replace-graph-from-file-job repo graph file)
                                                 {:job-desc (str "replace contents of graph " graph " from file")
                                                  :meta (api-routes/meta-params query-params)})))))))

(defn graph-management-routes [mount-point repo queue]
  (routes
   ;; deletes data in the graph. This could be a live or a draft graph.
   ;; accepts extra meta- query string params, which are added to queue metadata
   (DELETE mount-point {{graph "graph"} :query-params
                        query-params :query-params}
           (api-routes/when-params [graph]
                                   (enqueue-job! queue (delete-graph-job repo graph)
                                                 {:job-desc (str "delete graph" graph)
                                                  :meta (api-routes/meta-params query-params)})))
   (context
    mount-point []

    ;; makes a graph live.
    ;; accepts extra meta- query string params, which are added to queue metadata
    (PUT "/live" {{graph "graph"} :query-params
                  query-params :query-params}
         (api-routes/when-params [graph]
                                 (enqueue-job! queue (migrate-graph-live-job repo graph)
                                               {:job-desc (str "migrate graph " graph " to live")
                                                :meta (api-routes/meta-params query-params)}))))))
