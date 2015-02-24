(ns drafter.routes.drafts-api.jobs
  (:require [clojure.tools.logging :as log]
            [drafter.common.api-routes :as restapi]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.write-scheduler :refer [create-job]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [grafter.rdf.repository :refer [->connection query
                                            with-transaction]]))

(defn create-draft-job [repo live-graph params]
  (create-job :sync
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

  (create-job :sync
              (fn []
                (with-transaction (->connection repo)
                  (mgmt/delete-graph-and-draft-state! repo graph))
                (restapi/api-response 200 nil))))

(defn replace-graph-from-file-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified file."

  [repo graph {:keys [tempfile size filename content-type] :as file} metadata]
  (create-job :batch
              (fn []
                (log/info (str "Replacing graph " graph " with contents of file " tempfile "[" filename " " size " bytes]"))
                (with-transaction (->connection repo)
                  (mgmt/replace-data! repo graph (mimetype->rdf-format content-type)
                                      (:tempfile file)
                                      metadata))
                (log/info (str "Replaced graph " graph " with file " tempfile "[" filename "]")))
              (restapi/api-response 200 {:type :ok})))

(defn append-data-to-graph-from-file-job
  "Return a job function that adds the triples from the specified file
  to the specified graph."
  [repo graph {:keys [tempfile size filename content-type] :as file} metadata]

  (create-job :batch
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
  "Return a job function that adds the triples from the specified named graph to the specified graph"
  [repo graph source-graph metadata]
  (create-job :batch
              (fn []
                (log/info (str "Appending contents of " source-graph "  to graph: " graph))

                (let [query-str (str "CONSTRUCT { ?s ?p ?o } WHERE
                                      { GRAPH <" source-graph "> { ?s ?p ?o } }")
                      source-data (query repo query-str)]

                  (with-transaction (->connection repo)
                    (mgmt/append-data! repo graph source-data metadata))

                  (log/info (str "Graph import complete. Imported contents of " source-graph " to graph: " graph))
                  (restapi/api-response 200 {:type :ok})))))

(defn replace-data-from-graph-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified source graph."
  [repo graph source-graph metadata]
  (create-job :batch
              (fn []
                (log/info (str "Replacing graph " graph " with contents of graph: " source-graph))
                (with-transaction (->connection repo)
                  ;; TODO might want to batch this operation - though
                  ;; batching graph -> graph copies might be hard if
                  ;; we're also to allow writes in.
                  (mgmt/copy-graph repo source-graph graph)
                  (mgmt/add-metadata-to-graph repo graph metadata))
                (restapi/api-response 200 {:type :ok}))))

(defn migrate-graph-live-job [repo graph]
  (create-job :make-live
              (fn []
                (with-transaction (->connection repo)
                  (if (instance? String graph)
                    (mgmt/migrate-live! repo graph)
                    (doseq [g graph]
                      (mgmt/migrate-live! repo g))))
                (restapi/api-response 200 {:type :ok}))))
