(ns drafter.rdf.draft-management.jobs
  (:require [clojure.tools.logging :as log]
            [drafter.common.api-routes :as restapi]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.write-scheduler :refer [create-job submit-job! complete-job!]]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.io :refer [mimetype->rdf-format]]
            [grafter.rdf.repository :refer [->connection query
                                            with-transaction]]
            [environ.core :refer [env]]))

(defn batched-write-size []
  (Integer/parseInt (get env :drafter-batched-write-size "10000")))

(defmacro with-job-exception-handling [job & forms]
  `(try
     ~@forms
     (catch clojure.lang.ExceptionInfo exi#
       (complete-job! ~job {:type :error
                            :error-type (str (class exi#))
                            :exception (.getCause exi#)}))
     (catch Exception ex#
       (complete-job! ~job {:type :error
                            :error-type (str (class ex#))
                            :exception ex#}))))

(defmacro make-job [write-priority [job :as args] & forms]
  `(create-job ~write-priority
               (fn [~job]
                 (with-job-exception-handling ~job
                   ~@forms))))

(defn create-draft-job [repo live-graph params]
  (make-job :sync-write [job]
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

  (make-job :exclusive-write [job]
            (let [conn (->connection repo)]
              (with-transaction conn
                (mgmt/delete-graph-and-draft-state! conn graph)))

            (complete-job! job restapi/ok-response)))

(defn replace-graph-from-file-job
  "Return a function to replace the specified graph with a graph
  containing the tripes from the specified file.

  This operation is batched at the :batch-write level to allow
  cooperative scheduling with :sync-writes."

  [repo graph {:keys [tempfile size filename content-type] :as file} metadata]

  (make-job :batch-write [job]
            (log/info (str "Replacing graph " graph " with contents of file "
                           tempfile "[" filename " " size " bytes]"))

            (let [conn (->connection repo)]
              (with-transaction conn
                (mgmt/replace-data! conn graph (mimetype->rdf-format content-type)
                                    (:tempfile file)
                                    metadata)))
            (log/info (str "Replaced graph " graph " with file " tempfile "[" filename "]"))
            (complete-job! job restapi/ok-response)))

(defn- append-data-in-batches [repo draft-graph metadata triples job]
  (with-job-exception-handling job
    (let [conn (->connection repo)
          [current-batch remaining-triples] (split-at (batched-write-size) triples)]

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
          (mgmt/add-metadata-to-graph conn draft-graph metadata)
          (log/info (str "File import (append) to draft-graph: " draft-graph " completed"))
          (complete-job! job restapi/ok-response))))))

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
                                :buffer-size (batched-write-size))

        ;; NOTE that this is technically not transactionally safe as
        ;; sesame currently only supports the READ_COMMITTED isolation
        ;; level.
        ;;
        ;; As there is no read lock or support for (repeatable reads)
        ;; this means that the CONSTRUCT below can witness data
        ;; changing underneath it.
        ;;
        ;; TODO: protect against this, either by adopting a better
        ;; storage engine or by adding code to either refuse make-live
        ;; operations on jobs that touch the same graphs that we're
        ;; manipulating here, or to raise an error on the batch task.
        ;;
        ;; http://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Read_committed
        ;;
        ;; This can occur if a user does a make-live on a graph
        ;; which is being written to in a batch job.

        triples (lazy-cat (query repo
                                 (str "CONSTRUCT { ?s ?p ?o } "
                                      "WHERE { "
                                      (mgmt/with-state-graph
                                        "?live <" rdf:a "> <" drafter:ManagedGraph "> ;"
                                        "      <" drafter:hasDraft "> <" draft-graph "> .")
                                      "  GRAPH ?live { "
                                      "    ?s ?p ?o . "
                                      "  }"
                                      "}"))

                          new-triples)]

    (create-job :batch-write
                (partial append-data-in-batches repo
                         draft-graph metadata triples))))

(defn migrate-graph-live-job [repo graph]
  (make-job :exclusive-write [job]
            (log/info "Starting make-live for graph" graph)
            (let [conn (->connection repo)]
              (with-transaction conn
                (if (instance? String graph)
                  (mgmt/migrate-live! conn graph)
                  (doseq [g graph]
                    (mgmt/migrate-live! conn g)))))
            (log/info "Make-live for graph" graph "done")
            (complete-job! job restapi/ok-response)))
