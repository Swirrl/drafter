(ns drafter.backend.sesame.common.draft-api
  (:require [clojure.tools.logging :as log]
            [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add statements context]]
            [grafter.rdf.protocols :refer [map->Quad]]
            [drafter.rdf.draft-management.jobs :as jobs]
            [swirrl-server.async.jobs :refer [create-job create-child-job]]
            [drafter.common.api-routes :refer [meta-params]]
            [drafter.write-scheduler :as scheduler]
            [drafter.draftset :as ds]
            [drafter.rdf.sesame :refer [read-statements]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.backend.protocols :as backend]
            [drafter.util :as util]
            [drafter.backend.common.draft-api :refer [quad-batch->graph-triples]]
            [drafter.backend.sesame.common.protocols :refer :all]))

(defn delete-graph!
  "Deletes graph contents as per batch size in order to avoid blocking
  writes with a lock."
  [backend graph-uri contents-only? job]

  (let [drop-statement (str "DROP SILENT GRAPH <" graph-uri ">")]

    (if contents-only?
      (mgmt/update! backend drop-statement)
      (mgmt/update! backend (str drop-statement " ; "
                            (mgmt/delete-draft-state-query graph-uri))))

    (jobs/job-succeeded! job)))

(defn delete-graph-job [this graph-uri contents-only?]
  (log/info "Starting deletion job")
  (create-job :batch-write
              (partial delete-graph! this graph-uri contents-only?)))

(defn- append-data-batch-joblet [repo draft-graph batch]
  (jobs/action-joblet
    (log/info "Adding a batch of triples to repo")
    (mgmt/append-data-batch! repo draft-graph batch)))

(defn- append-graph-metadata-joblet [repo draft-graph metadata]
  (jobs/action-joblet
   (mgmt/append-graph-metadata! repo draft-graph metadata)
    (log/info (str "File import (append) to draft-graph: " draft-graph " completed"))))

(defn- append-draftset-quads [backend draftset-ref live->draft quad-batches {:keys [op job-started-at] :as state} job]
  (case op
    :append
    (if-let [batch (first quad-batches)]
      (let [{:keys [graph-uri triples]} (quad-batch->graph-triples batch)]
        (if-let [draft-graph-uri (get live->draft graph-uri)]
          (do
            (mgmt/set-modifed-at-on-draft-graph! backend draft-graph-uri job-started-at)
            (mgmt/append-data-batch! backend draft-graph-uri triples)
            (let [next-job (create-child-job
                            job
                            (partial append-draftset-quads backend draftset-ref live->draft (rest quad-batches) (merge state {:op :append})))]
              (scheduler/queue-job! next-job)))
          ;;NOTE: do this immediately instead of scheduling a
          ;;continuation since we haven't done any real work yet
          (append-draftset-quads backend draftset-ref live->draft quad-batches (merge state {:op :copy-graph :graph graph-uri}) job)))
      (jobs/job-succeeded! job))

    :copy-graph
    (let [live-graph-uri (:graph state)
          ds-uri (str (ds/->draftset-uri draftset-ref))
          {:keys [draft-graph-uri graph-map]} (mgmt/ensure-draft-exists-for backend live-graph-uri live->draft ds-uri)
          clone-batches (jobs/get-graph-clone-batches backend live-graph-uri)
          copy-batches-state (merge state {:op :copy-graph-batches
                                           :graph live-graph-uri
                                           :draft-graph draft-graph-uri
                                           :batches clone-batches})]
      ;;NOTE: do this immediately since we still haven't done any real work yet...
      (append-draftset-quads backend draftset-ref graph-map quad-batches copy-batches-state job))

    :copy-graph-batches
    (let [{:keys [graph batches draft-graph]} state]
      (if-let [[offset limit] (first batches)]
        (do
          (jobs/copy-graph-batch! backend graph draft-graph offset limit)
          (let [next-state (update-in state [:batches] rest)
                next-job (create-child-job
                          job
                          (partial append-draftset-quads backend draftset-ref live->draft quad-batches next-state))]
            (scheduler/queue-job! next-job)))
        ;;graph copy completed so continue appending quads
        ;;NOTE: do this immediately since we haven't done any work on this iteration
        (append-draftset-quads backend draftset-ref live->draft quad-batches (merge state {:op :append}) job)))))

(defn- append-quads-to-draftset-job [backend draftset-ref quads]
  (let [graph-map (dsmgmt/get-draftset-graph-mapping backend draftset-ref)
        quad-batches (util/batch-partition-by quads context jobs/batched-write-size)
        now (java.util.Date.)
        append-data (partial append-draftset-quads backend draftset-ref graph-map quad-batches {:op :append
                                                                                                :job-started-at now })]
    (create-job :batch-write append-data)))

(defn append-data-to-draftset-job [backend draftset-ref tempfile rdf-format]
  (append-quads-to-draftset-job backend draftset-ref (read-statements tempfile rdf-format)))

(defn append-triples-to-draftset-job [backend draftset-ref tempfile rdf-format graph]
  (let [triples (read-statements tempfile rdf-format)
        quads (map (comp map->Quad #(assoc % :c graph)) triples)]
    (append-quads-to-draftset-job backend draftset-ref quads)))

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

  (let [new-triples (read-statements tempfile rdf-format)

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
