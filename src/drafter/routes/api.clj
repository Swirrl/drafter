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

(defn add-import-job!
  "Adds a file import job to the queue and either appends the contents
  of the given file or replaces the graph with the given file
  depending upon the value of add-or-append which should be
  either :append-file or :replace-with-file."

  [queue file graph-uri action]
  (let [job (merge file {:action action
                         :graph-uri graph-uri})]
    (if-let [queue-id (q/offer! queue job)]
      (api-response 202 {:queue-id queue-id
                         :msg "Your import request was accepted"})
      (error-response 503 {:msg "The import queue is temporarily full.  Please try again later."}))))

(defn import-file! [repo {:keys [filename size tempfile action graph-uri] :as job}]
  (let [format (ses/filename->rdf-format filename)]
    (timbre/info (str "Importing file " tempfile "[" filename " " size " bytes] to graph: " graph-uri))

    (case action
      :append-file
      (ses/with-transaction repo
        (add repo
             graph-uri
             (statements tempfile :format format)))

      :replace-with-file
      (ses/with-transaction repo
        (mgmt/replace-data! repo
                            graph-uri
                            (statements tempfile :format format))))

    (timbre/info (str "File import complete " tempfile " to graph: " graph-uri))))

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

(defn api-routes [repo queue]
  (routes
   (POST "/draft/create" {{live-graph "live-graph"} :query-params}

         (when-params [live-graph]
           (let [draft-graph-uri (ses/with-transaction repo
                                   (mgmt/create-managed-graph! repo live-graph)
                                   (mgmt/create-draft-graph! repo live-graph))]
             (api-response 201 {:guri draft-graph-uri}))))

   (POST "/draft" {{graph "graph"} :query-params
                   {file :file} :params}

         (when-params [graph file]
           (add-import-job! queue file graph :append-file)))

   (PUT "/draft" {{graph "graph"} :query-params
                  {file :file} :params}

        (when-params [graph file]
                     (add-import-job! queue file graph :replace-with-file)))

   (DELETE "/draft" {{graph "graph"} :query-params}
           (when-params [graph]
             (ses/with-transaction repo
               (mgmt/delete-graph! graph)
               (api-response 200 {:msg "Graph deleted"}))))

   (PUT "/live" request
        {:status 202
         :headers {"Content-Type" "application/json"}
         :body ""}
        )

   ;; TODO add queue mangement API
   ))
