(ns drafter.routes.drafts-api.jobs
  (:require [clojure.tools.logging :as log]
            [drafter.common.api-routes :as restapi]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.write-scheduler :refer [create-job]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [grafter.rdf.repository :refer [->connection query
                                            with-transaction]]))

(defn create-draft-job [repo live-graph params]
  (create-job :sync-write
              (fn []
                (let [conn (->connection repo)
                      draft-graph-uri (with-transaction conn
                                        (mgmt/create-managed-graph! conn live-graph)
                                        (mgmt/create-draft-graph! conn
                                                                  live-graph (restapi/meta-params params)))]

                  (restapi/api-response 201 {:guri draft-graph-uri})))))

(defn delete-graph-job [repo graph]
  ;; TODO consider that we should really make this batchable - as its
  ;; mostly for deleting a draft graph and all other draft-graph
  ;; operations (except creation are batched).
  ;;
  ;; Otherwise deletes could block syncs.
  ;;
  ;; Could possibly implement with:
  ;;
  ;; DELETE { GRAPH <http://foo> { ?s ?p ?o } } WHERE { SELECT ?s ?p ?o WHERE { GRAPH <http://foo> { ?s ?p ?o } LIMIT 5000 }}
  ;;
  ;; And loop until the graph is empty?

  (create-job :sync-write
              (fn []
                (let [conn (->connection repo)]
                  (with-transaction conn
                    (mgmt/delete-graph-and-draft-state! conn graph)))
                (restapi/api-response 200 nil))))

(defn replace-graph-from-file-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified file.

  This operation is batched at the :batch-write level to allow
  cooperative scheduling with :sync-writes."

  [repo graph {:keys [tempfile size filename content-type] :as file} metadata]
  (create-job :batch-write
              (fn []
                (log/info (str "Replacing graph " graph " with contents of file " tempfile "[" filename " " size " bytes]"))
                (let [conn (->connection repo)]
                  (with-transaction conn
                    (mgmt/replace-data! conn graph (mimetype->rdf-format content-type)
                                        (:tempfile file)
                                        metadata)))
                (log/info (str "Replaced graph " graph " with file " tempfile "[" filename "]")))
              (restapi/api-response 200 {:type :ok})))

(defn append-data-to-graph-from-file-job
  "Return a job function that adds the triples from the specified file
  to the specified graph.

  This operation is batched at the :batch-write level to allow
  cooperative scheduling with :sync-writes."
  [repo graph {:keys [tempfile size filename content-type] :as file} metadata]

  (create-job :batch-write
              (fn []
                (let [conn (->connection repo)]
                  (log/info (str "Appending contents of file " tempfile "[" filename " " size " bytes] to graph: " graph " via connection " conn " on repo " repo))
                  (with-transaction conn
                    (mgmt/append-data! conn graph (mimetype->rdf-format content-type)
                                       (:tempfile file)
                                       metadata))
                  (log/info (str "File import (append) complete " tempfile " to graph: " graph))
                  (restapi/api-response 200 {:type :ok})))))

(defn append-data-to-graph-from-graph-job
  "Return a job function that adds the triples from the specified
  named graph to the specified graph.

  This operation can't be batched so must occur at an exclusive-write
  level."
  [repo graph source-graph metadata]
  (create-job :exclusive-write
              (fn []
                (log/info (str "Appending contents of " source-graph "  to graph: " graph))

                (let [query-str (str "CONSTRUCT { ?s ?p ?o } WHERE
                                      { GRAPH <" source-graph "> { ?s ?p ?o } }")
                      conn (->connection repo)
                      source-data (query repo query-str)]

                  (with-transaction conn
                    ;; append-data also adds metadata too
                    (mgmt/append-data! conn graph source-data metadata))

                  (log/info (str "Graph import complete. Imported contents of " source-graph " to graph: " graph))
                  (restapi/api-response 200 {:type :ok})))))

(defn replace-data-from-graph-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified source graph.

  This operation can't be batched so must occur at an exclusive-write
  level."

  [repo graph source-graph metadata]

  (create-job :exclusive-write
              (fn []
                (log/info (str "Replacing graph " graph " with contents of graph: " source-graph))
                (let [conn (->connection repo)]
                  (with-transaction conn
                    (mgmt/copy-graph conn source-graph graph)
                    (mgmt/add-metadata-to-graph conn graph metadata)))
                (restapi/api-response 200 {:type :ok}))))

(defn migrate-graph-live-job [repo graph]
  (create-job :exclusive-write
              (fn []
                (let [conn (->connection repo)]
                  (with-transaction
                    (if (instance? String graph)
                      (mgmt/migrate-live! conn graph)
                      (doseq [g graph]
                        (mgmt/migrate-live! conn g)))))
                (restapi/api-response 200 {:type :ok}))))
