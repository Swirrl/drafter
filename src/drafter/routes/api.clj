(ns drafter.routes.api
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
            [grafter.rdf.sesame :as ses]
            [grafter.rdf.protocols :refer [add statements]])
  (:import [org.openrdf.query.resultio TupleQueryResultFormat BooleanQueryResultFormat]))

(def default-response-map {:type :ok})

(def default-error-map {:type :error :msg "An unknown error occured"})

(defn api-response
  [code map]
  {:status code
   :headers {"Content-Type" "application/json"}
   :body (merge default-response-map map)})

(defn error-response
  [code map]
  (api-response code (merge default-error-map map)))

(defn enqueue-job!
  "Enqueues a job function on the queue and returns a ring HTTP
  response."

  [queue job-function]
  (let []
    (if-let [queue-id (q/offer! queue job-function)]
      (api-response 202 {:queue-id queue-id
                         :msg "Your import request was accepted"})
      (error-response 503 {:msg "The import queue is temporarily full.  Please try again later."}))))

(def no-file-or-graph-param-error-msg {:msg "You must supply both a 'file' and 'graph' parameter."})

(defmacro when-params
  "Simple macro that takes a set of paramaters and tests that they're
  all truthy.  If any are falsey it returns an appropriate ring
  response with an error message.  The error message assumes that the
  symbol name is the same as the HTTP parameter name."
  [params & form]
  `(if (every? identity ~params)
     ~@form
     (error-response 400 {:msg (str "You must supply the parameters " ~(->> params
                                                                            (interpose ", ")
                                                                            (apply str)))})))

(defn replace-graph-from-file-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified file."
  [repo graph {:keys [tempfile size filename] :as file}]
  (fn []
    (let [format (ses/filename->rdf-format filename)]
      (timbre/info (str "Replacing graph " graph " with contents of file " tempfile "[" filename " " size " bytes]"))

      (ses/with-transaction repo
        (mgmt/replace-data! repo
                            graph
                            (statements tempfile :format format)))

      (timbre/info (str "Replaced graph " graph " with file " tempfile "[" filename "]")))))

(defn append-data-to-graph-from-file-job
  "Return a job function that adds the triples from the specified file
  to the specified graph."
  [repo graph {:keys [tempfile size filename] :as file}]
  (fn []
    (let [format (ses/filename->rdf-format filename)]
      (timbre/info (str "Appending contents of file " tempfile "[" filename " " size " bytes] to graph: " graph))

      (ses/with-transaction repo
        (add repo
             graph
             (statements (:tempfile file) :format format)))

      (timbre/info (str "File import (append) complete " tempfile " to graph: " graph)))))

(defn append-data-to-graph-from-graph-job
  [repo graph source-graph]
  "Return a job function that adds the triples from the specified named graph to the specified graph"
  (fn []
    (timbre/info (str "Appending contents of " source-graph "  to graph: " graph))

    (let [query-str (str "CONSTRUCT { ?s ?p ?o } WHERE
                         { GRAPH <" source-graph "> { ?s ?p ?o } }")
          source-data (ses/query repo query-str)]
      (ses/with-transaction repo (add repo graph source-data))
      (timbre/info (str "Graph import complete. Imported contents of " source-graph " to graph: " graph)))))

(defn replace-data-from-graph-job
  [repo graph source-graph]
  (fn []
    (timbre/info (str "Replacing graph " graph " with contents of graph: " source-graph ))

    (let [query-str (str "CONSTRUCT { ?s ?p ?o } WHERE
                         { GRAPH <" source-graph "> { ?s ?p ?o } }")
          source-data (ses/query repo query-str)]


      (if source-data
        ; there's some data in the source graph
        (do
          (ses/with-transaction repo
            (mgmt/replace-data! repo
                            graph
                            source-data))

          (timbre/info (str "Graph replace complete. Replaced contents of " source-graph " into graph: " graph)))

        ; no data in source graph
        (do
          (println "NO SOURCE DATA!")
          (ses/with-transaction repo
            (mgmt/delete-graph! repo graph))

          (timbre/info (str "Source graph " source-graph " was empty. Deleted destination graph.")))))))

(defn delete-graph-job [repo graph]
  (fn []
    (ses/with-transaction repo
      (mgmt/delete-graph! repo graph))))

(defn migrate-graph-live-job [repo graph]
  (fn []
    (ses/with-transaction repo
      (mgmt/migrate-live! repo graph))))

(defn api-routes [repo queue]
  (routes
   (POST "/draft/create" {{live-graph "live-graph"} :query-params}

         (when-params [live-graph]
           (let [draft-graph-uri (ses/with-transaction repo
                                   (mgmt/create-managed-graph! repo live-graph)
                                   (mgmt/create-draft-graph! repo live-graph))]
             (api-response 201 {:guri draft-graph-uri}))))

   (POST "/draft" {{graph "graph" source-graph "source-graph"} :query-params
                   {file :file} :params}

         (if source-graph
           ; when source supplied: append from source-graph.
           (when-params [graph source-graph]
                      (enqueue-job! queue (append-data-to-graph-from-graph-job repo graph source-graph)))
           ; when source graph not supplied: append from the file.
           (when-params [graph file]
                      (enqueue-job! queue (append-data-to-graph-from-file-job repo graph file)))))

   (PUT "/draft" {{graph "graph" source-graph "source-graph"} :query-params
                  {file :file} :params}

        (if source-graph

            ; when source supplied: replace from source-graph.
            (when-params [graph source-graph]
                     (enqueue-job! queue (replace-data-from-graph-job repo graph source-graph)))

            ; when source graph not supplied: replace from the file.
            (when-params [graph file]
                      (enqueue-job! queue (replace-graph-from-file-job repo graph file)))))


   (DELETE "/graph" {{graph "graph"} :query-params}
           (when-params [graph]
                        (enqueue-job! queue (delete-graph-job repo graph))))

   (PUT "/live" {{graph "graph"} :query-params}
        (when-params [graph]
                     (enqueue-job! queue (migrate-graph-live-job repo graph))))))
