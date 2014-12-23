(ns drafter.routes.drafts-api
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [ring.util.io :as io]
            [ring.middleware.multipart-params]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]]
            [drafter.rdf.draft-management :as mgmt]
            [clojure.tools.logging :as log]
            [drafter.rdf.sparql-protocol :refer [process-sparql-query]]
            [drafter.common.api-routes :as api-routes]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [grafter.rdf.repository :refer [->connection with-transaction query]]
            [grafter.rdf :refer [statements]])
  (:import [org.openrdf.query.resultio TupleQueryResultFormat BooleanQueryResultFormat]
           [org.openrdf.rio RDFParseException]
           [org.openrdf.repository Repository RepositoryConnection]))

(defn exec-job!
  "Executes a job function straight away and returns a ring HTTP
  response."
  ;; state should be an atom
  [state job-function opts]
  (let [job-id (java.util.UUID/randomUUID)]
    (try
      (swap! state assoc job-id opts)
      ; TODO make the job function return the status code and message.
      (job-function)
      (api-routes/api-response 200 {:msg "Your job executed succesfully"})

      (catch RDFParseException ex
        (api-routes/api-response 400 {:msg (str "Invalid RDF provided: " ex)}))
      (catch clojure.lang.ExceptionInfo ex
        (cond
         (= :reading-aborted (-> ex ex-data :type)) (api-routes/api-response 400 {:msg (str "Invalid RDF provided: " ex)})
         :else (do
                 (log/error "Unknown error " ex)
                 (api-routes/api-response 500 {:msg (str "Unknown error: " ex)}))
         ))
      (finally
        (swap! state dissoc job-id)))))

(def no-file-or-graph-param-error-msg {:msg "You must supply both a 'file' and 'graph' parameter."})

(defn replace-graph-from-file-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified file."
  [repo graph {:keys [tempfile size filename content-type] :as file} metadata]
  (fn []
    (log/info (str "Replacing graph " graph " with contents of file " tempfile "[" filename " " size " bytes]"))
    (with-transaction repo
                      (mgmt/replace-data! repo graph (mimetype->rdf-format content-type)
                                         (:tempfile file)
                                         metadata))
    (log/info (str "Replaced graph " graph " with file " tempfile "[" filename "]"))))

(defn append-data-to-graph-from-file-job
  "Return a job function that adds the triples from the specified file
  to the specified graph."
  [repo graph {:keys [tempfile size filename content-type] :as file} metadata]
  (fn []
    (log/info (str "Appending contents of file " tempfile "[" filename " " size " bytes] to graph: " graph))
    (with-transaction repo
                      (mgmt/append-data! repo graph (mimetype->rdf-format content-type)
                                        (:tempfile file)
                                        metadata))
    (log/info (str "File import (append) complete " tempfile " to graph: " graph))))

(defn append-data-to-graph-from-graph-job
  "Return a job function that adds the triples from the specified named graph to the specified graph"
  [repo graph source-graph metadata]
  (fn []
    (log/info (str "Appending contents of " source-graph "  to graph: " graph))

    (let [query-str (str "CONSTRUCT { ?s ?p ?o } WHERE
                         { GRAPH <" source-graph "> { ?s ?p ?o } }")
          source-data (query repo query-str)]

      (with-transaction repo
        (mgmt/append-data! repo graph source-data metadata))

      (log/info (str "Graph import complete. Imported contents of " source-graph " to graph: " graph)))))

(defn replace-data-from-graph-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified source graph."
  [repo graph source-graph metadata]
  (fn []
    (log/info (str "Replacing graph " graph " with contents of graph: " source-graph ))

    (let [query-str (str "CONSTRUCT { ?s ?p ?o } WHERE
                         { GRAPH <" source-graph "> { ?s ?p ?o } }")
          source-data (query repo query-str)]
          (with-transaction repo
                            (mgmt/replace-data! repo graph source-data metadata))
          (log/info (str "Graph replace complete. Replaced contents of " source-graph " into graph: " graph)))))

(defn delete-graph-job [repo graph]
  (fn []
    (with-transaction repo
      (mgmt/delete-graph-and-draft-state! repo graph))))

(defn migrate-graph-live-job [repo graph]
  (fn []
    (with-transaction repo
      (if (instance? String graph)
        (mgmt/migrate-live! repo graph)
        (doseq [g graph]
          (mgmt/migrate-live! repo g))))))

(defn override-file-format
  "Takes a file object (hash) and if a non-nil file-format is supplied
  overrides its content-type."
  [file-format file-obj]
  (if file-format
    (assoc file-obj :content-type file-format)
    file-obj))

(defn draft-api-routes [mount-point repo state]
  (routes
   (context
    mount-point []

    ;; makes a new managed/draft graph.
    ;; accepts extra meta- query string params, which are added to the state graph

    (POST "/create" {{live-graph :live-graph} :params
                     params :params}
          (with-open [conn (->connection repo)]
            (api-routes/when-params [live-graph]
                                    (let [draft-graph-uri (with-transaction conn
                                                            (mgmt/create-managed-graph! conn live-graph)
                                                            (mgmt/create-draft-graph! conn live-graph (api-routes/meta-params params)))]
                                      (api-routes/api-response 201 {:guri draft-graph-uri}))))))

   ;; adds data to the graph from either source-graph or file
   ;; accepts extra meta- query string params, which are added to queue metadata
   (routes
    (POST mount-point {{graph :graph source-graph :source-graph} :params
                       {content-type :content-type} :params
                       query-params :query-params
                       {file :file} :params}

          (with-open [conn (->connection repo)]
            (let [metadata (api-routes/meta-params query-params)]
              (if source-graph
                (api-routes/when-params [graph source-graph] ; when source supplied: append from source-graph.
                                        (exec-job! state
                                                   (append-data-to-graph-from-graph-job conn graph source-graph metadata)
                                                   {:job-desc (str "append to graph: " graph " from source graph: " source-graph)
                                                    :meta metadata}))

                (api-routes/when-params [graph file] ; when source graph not supplied: append from the file.
                                        (exec-job! state
                                                   (append-data-to-graph-from-file-job conn graph (override-file-format content-type file) metadata)
                                                   {:job-desc (str "append to graph " graph " from file")
                                                    :meta metadata}))))))

      ;; replaces data in the graph from either source-graph or file
      ;; accepts extra meta- query string params, which are added to queue metadata

    (PUT mount-point {{graph :graph source-graph :source-graph} :params
                      {content-type :content-type} :params
                      query-params :query-params
                      {file :file} :params}
         (with-open [conn (->connection repo)]
           (let [metadata (api-routes/meta-params query-params)]
             (if source-graph
               (api-routes/when-params [graph source-graph] ; when source supplied: replace from source-graph.
                                       (exec-job! state
                                                  (replace-data-from-graph-job conn graph source-graph metadata)
                                                  {:job-desc (str "replace contents of graph " graph " from source graph: " source-graph)
                                                   :meta metadata}))

               (api-routes/when-params [graph file] ; when source graph not supplied: replace from the file.
                                       (exec-job! state
                                                  (replace-graph-from-file-job conn graph (override-file-format content-type file) metadata)
                                                  {:job-desc (str "replace contents of graph " graph " from file")
                                                   :meta metadata})))))))))

(defn graph-management-routes [mount-point repo state]
  (routes
   ;; deletes data in the graph. This could be a live or a draft graph.
   ;; accepts extra meta- query string params, which are added to queue metadata
   (DELETE mount-point {{graph :graph} :params
                        query-params :query-params}
           (with-open [conn (->connection repo)]
             (api-routes/when-params [graph]
                                     (exec-job! state (delete-graph-job conn graph)
                                                {:job-desc (str "delete graph" graph)
                                                 :meta (api-routes/meta-params query-params)}))))
   (context
    mount-point []

    ;; makes a graph live.
    ;; accepts extra meta- query string params, which are added to queue metadata
    (PUT "/live" {{graph :graph} :params
                  query-params :query-params
                  :as request}
         (log/info request)
         (with-open [conn (->connection repo)]
           (api-routes/when-params [graph]
                                   (exec-job! state (migrate-graph-live-job conn graph)
                                              {:job-desc (str "migrate graph " graph " to live")
                                               :meta (api-routes/meta-params query-params)})))))))
