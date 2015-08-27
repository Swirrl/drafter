(ns drafter.backend.sesame.common.draft-api
  (:require [clojure.tools.logging :as log]
            [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add statements]]
            [drafter.rdf.draft-management.jobs :as jobs]
            [swirrl-server.async.jobs :refer [create-job create-child-job]]
            [drafter.common.api-routes :refer [meta-params]]
            [drafter.write-scheduler :as scheduler]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.backend.protocols :as backend]
            [drafter.backend.sesame.common.protocols :refer :all]))

(def ^:private get-repo :repo)

(defn- finish-delete-job! [backend graph contents-only? job]
  (when-not contents-only?
    (mgmt/delete-draft-graph-state! backend graph))
  (jobs/job-succeeded! job))

(defn- delete-in-batches [backend graph contents-only? job]
  ;; Loops until the graph is empty, then deletes state graph if not a
  ;; contents-only? deletion.
  ;;
  ;; Checks that graph is a draft graph - will only delete drafts.
  (if (and (mgmt/graph-exists? backend graph)
           (mgmt/draft-exists? backend graph))
      (do
        (delete-graph-batch! backend graph jobs/batched-write-size)

        (if (mgmt/graph-exists? backend graph)
          ;; There's more graph contents so queue another job to continue the
          ;; deletions.
          (let [apply-next-batch (partial delete-in-batches backend graph contents-only?)]
            (scheduler/queue-job! (create-child-job job apply-next-batch)))
          (finish-delete-job! backend graph contents-only? job)))
      (finish-delete-job! backend graph contents-only? job)))

(defn delete-graph-job [this graph-uri contents-only?]
  (log/info "Starting batch deletion job")
  (create-job :batch-write
              (partial delete-in-batches this graph-uri contents-only?)))

(defn- append-data-in-batches [repo draft-graph metadata triples job]
  (jobs/with-job-exception-handling job
    (let [[current-batch remaining-triples] (split-at jobs/batched-write-size triples)]

      (log/info (str "Adding a batch of triples to repo" current-batch))
      (backend/append-data-batch! repo draft-graph current-batch)

      (if-not (empty? remaining-triples)
        ;; resubmit the remaining batches under the same job to the
        ;; queue to give higher priority jobs a chance to write
        (let [apply-next-batch (partial append-data-in-batches
                                        repo draft-graph metadata remaining-triples)]
          (scheduler/queue-job! (create-child-job job apply-next-batch)))

        (do
          (backend/append-graph-metadata! repo draft-graph metadata)
          (log/info (str "File import (append) to draft-graph: " draft-graph " completed"))

          (jobs/job-succeeded! job))))))

(defn append-data-to-graph-job
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

  [backend draft-graph tempfile rdf-format metadata]

  (let [new-triples (statements tempfile
                                :format rdf-format
                                :buffer-size jobs/batched-write-size)

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
        ;; I think the newer versions of Sesame 1.8.x might also provide better
        ;; support for different isolation levels, so we might want to consider
        ;; upgrading.
        ;;
        ;; http://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Read_committed
        ;;
        ;; This can occur if a user does a make-live on a graph
        ;; which is being written to in a batch job.
    ]

    (jobs/make-job :batch-write [job]
              (append-data-in-batches backend draft-graph metadata new-triples job))))

(defn new-draft-job [backend live-graph params]
  (jobs/make-job :sync-write [job]
            (with-open [conn (repo/->connection (get-repo backend))]
              (let [draft-graph-uri (repo/with-transaction conn
                                      (mgmt/create-managed-graph! conn live-graph)
                                      (mgmt/create-draft-graph! conn live-graph (meta-params params)))]
                (jobs/job-succeeded! job {:guri draft-graph-uri})))))

(defn copy-from-live-graph-job [backend draft-graph-uri]
  (jobs/create-copy-from-live-graph-job (get-repo backend) draft-graph-uri))

(defn delete-metadata-job [backend graphs meta-keys]
  (jobs/create-delete-metadata-job (get-repo backend) graphs meta-keys))

(def create-update-metadata-job jobs/create-update-metadata-job)
