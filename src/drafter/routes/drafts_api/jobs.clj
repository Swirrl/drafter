(ns drafter.routes.drafts-api.jobs
  (:require [clojure.tools.logging :as log]
            [drafter.common.api-routes :as restapi]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.write-scheduler :refer [create-job submit-job! complete-job!]]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf.ontologies.rdf :refer :all]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [grafter.rdf.repository :refer [->connection query
                                            with-transaction]]))

(def batched-write-size 10000)

(defmacro make-job [write-priority [job writes-queue :as args] & forms]
  `(create-job ~write-priority
               (fn [~job ~writes-queue]
                 (try
                   ~@forms
                   (catch Exception ex#
                     (complete-job! ~job {:type :error
                                          :exception ex#}))))))

(defn create-draft-job [repo live-graph params]
  (make-job :sync-write [job writes-queue]
            (let [conn (->connection repo)
                  draft-graph-uri (with-transaction conn
                                    (mgmt/create-managed-graph! conn live-graph)
                                    (mgmt/create-draft-graph! conn
                                                              live-graph (restapi/meta-params params)))]

              (complete-job! job (restapi/api-response 201 {:guri draft-graph-uri})))))

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

  (make-job :sync-write [job writes-queue]
            (let [conn (->connection repo)]
              (with-transaction conn
                (mgmt/delete-graph-and-draft-state! conn graph)))

            (complete-job! job (restapi/api-response 200 nil))))

(defn replace-graph-from-file-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified file.

  This operation is batched at the :batch-write level to allow
  cooperative scheduling with :sync-writes."

  [repo graph {:keys [tempfile size filename content-type] :as file} metadata]

  (make-job :batch-write [job writes-queue]
            (log/info (str "Replacing graph " graph " with contents of file "
                           tempfile "[" filename " " size " bytes]"))

            (let [conn (->connection repo)]
              (with-transaction conn
                (mgmt/replace-data! conn graph (mimetype->rdf-format content-type)
                                    (:tempfile file)
                                    metadata)))
            (log/info (str "Replaced graph " graph " with file " tempfile "[" filename "]"))
            (complete-job! job (restapi/api-response 200 {:type :ok}))))

(defn append-data-in-batches [repo draft-graph metadata triples job writes-queue]
  (let [conn (->connection repo)
        [current-batch remaining-triples] (split-at batched-write-size triples)]

    (log/info (str "Adding a batch of triples to repo" current-batch))
    (with-transaction conn
      (mgmt/append-data! conn draft-graph current-batch))

    (if-not (empty? remaining-triples)
      ;; resubmit the remaining batches under the same job to the
      ;; queue to give higher priority jobs a chance to write
      (let [apply-next-batch (partial append-data-in-batches repo draft-graph metadata
                                      remaining-triples)]
        (submit-job! (assoc job :function apply-next-batch)))

      (do
        ;; TODO Add metadata triples
        (log/info (str "File import (append) to draft-graph: " draft-graph " completed"))
        (complete-job! job (restapi/api-response 200 {:type :ok}))))))

(defn append-data-to-graph-from-file-job
  "Return a job function that adds the triples from the specified file
  to the specified graph.

  This operation is batched at the :batch-write level to allow
  cooperative scheduling with :sync-writes.

  It works by concatenating the existing live quads with a lazy-seq on
  the uploaded file.  This combined lazy sequence is then split into
  the current batch and remaining, with the current batch being
  applied before the job is resubmitted (under the same ID) with the
  remaining triples.

  The last batch is finally responsible for signaling job completion
  via a side-effecting call to complete-job!"

  [repo draft-graph {:keys [tempfile size filename content-type] :as file} metadata]

  (let [new-triples (statements tempfile
                                :format (mimetype->rdf-format content-type)
                                :buffer-size batched-write-size)

        triples (lazy-cat new-triples (query repo
                                             (str "CONSTRUCT { ?s ?p ?o } "
                                                  "WHERE { "
                                                  (mgmt/with-state-graph
                                                    "?live <" rdf:a "> <" drafter:ManagedGraph "> ;"
                                                    "      <" drafter:hasDraft "> <" draft-graph "> .")
                                                  "  GRAPH ?live { "
                                                  "    ?s ?p ?o . "
                                                  "  }"
                                                  "}")))]

    (create-job :batch-write
                (partial append-data-in-batches repo draft-graph metadata
                         triples))))

(defn append-data-to-graph-from-graph-job
  "Return a job function that adds the triples from the specified
  named graph to the specified graph.

  This operation can't be batched so must occur at an exclusive-write
  level."

  [repo draft-graph source-graph metadata]

  (make-job :exclusive-write [job writes-queue]
            (log/info "Appending contents of " source-graph "  to draft-graph: " draft-graph)

            (let [query-str (str "CONSTRUCT { ?s ?p ?o } WHERE
                                      { GRAPH <" source-graph "> { ?s ?p ?o } }")
                  conn (->connection repo)
                  source-data (query repo query-str)]

              (with-transaction conn
                (mgmt/add-metadata-to-graph conn draft-graph)
                (mgmt/append-data! conn draft-graph source-data metadata))

              (log/info  "Graph import complete. Imported contents of " source-graph " to draft-graph: " draft-graph)
              (complete-job! job (restapi/api-response 200 {:type :ok})))))

(defn replace-data-from-graph-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified source graph.

  This operation can't be batched so must occur at an exclusive-write
  level."

  [repo graph source-graph metadata]

  (make-job :exclusive-write [job writes-queue]
            (log/info "Replacing graph " graph " with contents of graph: " source-graph)
            (let [conn (->connection repo)]
              (with-transaction conn
                (mgmt/copy-graph conn source-graph graph)
                (mgmt/add-metadata-to-graph conn graph metadata)))
            (complete-job! job (restapi/api-response 200 {:type :ok}))))

(defn migrate-graph-live-job [repo graph]
  (make-job :exclusive-write [job writes-queue]
            (let [conn (->connection repo)]
              (with-transaction conn
                (if (instance? String graph)
                  (mgmt/migrate-live! conn graph)
                  (doseq [g graph]
                    (mgmt/migrate-live! conn g)))))
            (complete-job! job (restapi/api-response 200 {:type :ok}))))
