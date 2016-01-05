(ns drafter.backend.sesame.common.draft-api
  (:require [clojure.tools.logging :as log]
            [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add statements context]]
            [drafter.rdf.draft-management.jobs :as jobs]
            [swirrl-server.async.jobs :refer [create-job create-child-job]]
            [drafter.common.api-routes :refer [meta-params]]
            [drafter.write-scheduler :as scheduler]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.backend.protocols :as backend]
            [drafter.util :as util]
            [drafter.backend.common.draft-api :refer [quad-batch->graph-triples]]
            [drafter.backend.sesame.common.protocols :refer :all]))

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

(defn- append-data-batch-joblet [repo draft-graph batch]
  (jobs/action-joblet
    (log/info "Adding a batch of triples to repo")
    (backend/append-data-batch! repo draft-graph batch)))

(defn- append-graph-metadata-joblet [repo draft-graph metadata]
  (jobs/action-joblet
   (backend/append-graph-metadata! repo draft-graph metadata)
    (log/info (str "File import (append) to draft-graph: " draft-graph " completed"))))

(defn- file->statements [tempfile rdf-format]
  (statements tempfile
              :format rdf-format
              :buffer-size jobs/batched-write-size))

(defn- append-data-to-draftset-graph-joblet [backend draftset-uri quad-batch]
  (fn [graph-map]
    (let [{:keys [graph-uri triples]} (quad-batch->graph-triples quad-batch)
          {:keys [draft-graph-uri graph-map]} (mgmt/ensure-draft-exists-for backend graph-uri graph-map draftset-uri)]
      (backend/append-data-batch! backend draft-graph-uri quad-batch)
      graph-map)))

(defn append-data-to-draftset-job [backend draftset-uri tempfile rdf-format]
  (let [quads (file->statements tempfile rdf-format)
        graph-map (dsmgmt/get-draftset-graph-mapping backend draftset-uri)
        quad-batches (util/batch-partition-by quads context jobs/batched-write-size)
        batch-joblets (map #(append-data-to-draftset-graph-joblet backend draftset-uri %) quad-batches)]
    (jobs/joblet-seq->job batch-joblets :batch-write graph-map)))

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

  (let [new-triples (file->statements tempfile rdf-format)

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
        batches (partition-all jobs/batched-write-size new-triples)
        batch-joblets (map #(append-data-batch-joblet backend draft-graph %) batches)
        metadata-joblet (append-graph-metadata-joblet backend draft-graph metadata)
        all-joblets (concat batch-joblets [metadata-joblet])
    ]
    (jobs/joblet-seq->job all-joblets :batch-write)))

(defn new-draft-job [backend live-graph params]
  (jobs/make-job :sync-write [job]
            (with-open [conn (repo/->connection (->sesame-repo backend))]
              (let [draft-graph-uri (repo/with-transaction conn
                                      (mgmt/create-managed-graph! conn live-graph)
                                      (mgmt/create-draft-graph! conn live-graph (meta-params params)))]
                (jobs/job-succeeded! job {:guri draft-graph-uri})))))

(defn copy-from-live-graph-job [backend draft-graph-uri]
  (jobs/create-copy-from-live-graph-job (->sesame-repo backend) draft-graph-uri))

(defn delete-metadata-job [backend graphs meta-keys]
  (jobs/create-delete-metadata-job (->sesame-repo backend) graphs meta-keys))

(def create-update-metadata-job jobs/create-update-metadata-job)
