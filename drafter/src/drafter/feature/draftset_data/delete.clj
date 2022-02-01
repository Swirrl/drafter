(ns drafter.feature.draftset-data.delete
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as ops]
            [drafter.backend.draftset.rewrite-result :as rer :refer [rewrite-statement]]
            [drafter.draftset :as ds]
            [drafter.feature.draftset-data.common :as ds-data-common]
            [drafter.feature.draftset-data.middleware :as deset-middleware]
            [drafter.middleware :refer [require-rdf-content-type temp-file-body inflate-gzipped]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sesame :refer [is-quads-format? read-statements]]
            [drafter.job-responses :as response]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.io :as gio]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [drafter.requests :as req]
            [drafter.async.responses :as async-response])
  (:import org.eclipse.rdf4j.model.Resource))

(defn- empty-draft-only-graph? [repo live-graph-uri draft-graph-uri]
  (let [empty? (mgmt/graph-empty? repo draft-graph-uri)
        live? (mgmt/is-graph-live? repo live-graph-uri)]
    (and empty? (not live?))))

(defn- delete-draft-batch
  "Rewrites an incoming quad batch according to the draftset graph mapping and deletes them from the
   corresponding draft graph."
  [repo live->draft batch]
  (with-open [conn (repo/->connection repo)]
    (let [rewritten-statements (map #(rer/rewrite-statement live->draft %) batch)
          sesame-statements (map gio/quad->backend-quad rewritten-statements)
          graph-array (into-array Resource (map util/uri->sesame-uri (vals live->draft)))]
      (.remove conn sesame-statements graph-array))))

(defn- delete-quad-batch
  "Deletes a quad batch within a draft graph. If the draft graph is empty after the delete it will be removed
   from the draftset. The modification time will be updated for the target graph unless the graph is removed
   in which case the graph will be removed from the modifications graph. If no user graphs remain within the
   draft modifications graph it will also be deleted. Returns the new operation state."
  [{:keys [live->draft] :as state} {:keys [draftset-ref] :as context} live-graph-uri draft-graph-uri batch]
  ;; delete triples
  (let [repo (ds-data-common/get-repo context)]
    (delete-draft-batch repo live->draft batch)

    ;; if the deleting graph only exists in the draft and becomes empty the associated draft graph should be removed
    ;; 1. remove the graph from the live->draft graph mapping if required
    ;; 2. update the modifications graph based on the new state
    ;; 3. rewrite the draftset contents. WARNING: This should only be done after all data changes have been made
    ;; 4. delete the draft graph if required
    (let [became-empty? (empty-draft-only-graph? repo live-graph-uri draft-graph-uri)
          state (if became-empty?
                  (ds-data-common/remove-draft-graph state live-graph-uri)
                  state)
          state (if became-empty?
                  (ds-data-common/remove-draft-only-graph-modified-time state context draft-graph-uri)
                  (ds-data-common/draft-graph-deletion state context draft-graph-uri))]

      ;; rewrite draft
      (with-open [conn (repo/->connection repo)]
        (mgmt/unrewrite-draftset! conn {:draftset-uri (ds/->draftset-uri draftset-ref)
                                        :deleted      :rewrite}))

      ;; if the deleted triples have caused the graph to become empty and the graph does not exist in live
      ;; it should be removed from the draft. WARNING: The graph should only be removed after the draft
      ;; has been rewritten
      (when became-empty?
        (mgmt/delete-draft-graph! (ds-data-common/get-repo context) draft-graph-uri))
      state)))

(defn- done! [{:keys [draftset-ref] :as context}]
  (let [repo (ds-data-common/get-repo context)
        draftset-info (ops/get-draftset-info repo draftset-ref)]
    (ds-data-common/done-state {:draftset draftset-info})))

(defn- delete-state [state context]
  (let [repo (ds-data-common/get-repo context)]
    (loop [quad-batches (:quad-batches state)]
      (if-let [batch (first quad-batches)]
        (let [live-graph (pr/context (first batch))]
          (if (mgmt/is-graph-managed? repo live-graph)
            (if-let [draft-graph-uri (ds-data-common/get-draft-graph state live-graph)]
              (let [state (delete-quad-batch state context live-graph draft-graph-uri batch)]
                (if-let [remaining-batches (next quad-batches)]
                  (assoc state :quad-batches remaining-batches)
                  (done! context)))
              (let [draft-graph-uri (ds-data-common/copy-user-graph context live-graph)]
                (-> state
                    (assoc :quad-batches quad-batches)
                    (ds-data-common/add-draft-graph live-graph draft-graph-uri))))
            ;;live graph does not exist so do not create a draft graph
            ;;NOTE: This is the same behaviour as deleting a live graph which does not exist in live
            ;;NOTE: no work has been done yet so keep searching for a batch in graph that exists
            (recur (next quad-batches))))
        (done! context)))))

(defn delete-state-machine
  "Returns a state machine for delete jobs"
  []
  (reify ds-data-common/StateMachine
    (create-initial-state [_this live->draft source]
      (ds-data-common/init-state ::delete live->draft source))
    (step [_this state context]
      (case (ds-data-common/state-label state)
        ::delete (delete-state state context)
        (ds-data-common/unknown-label state)))))

(defn delete-data-from-draftset-job
  "Creates a job to delete quads from a source into a draft on behalf of the given user"
  [{:keys [backend] :as manager} user-id draftset-id source metadata]
  (let [job-meta (jobs/job-metadata backend draftset-id 'delete-data-from-draftset metadata)
        sm (delete-state-machine)]
    (ds-data-common/create-state-machine-job manager user-id draftset-id source job-meta sm)))

(defn delete-data
  "Enqueues a job to delete the data within source from draftset on behalf of the given user"
  [manager user-id draftset source metadata]
  (let [job (delete-data-from-draftset-job manager user-id draftset source metadata)]
    (response/enqueue-async-job! job)))

(defn delete-draftset-data-handler
  [{:keys [:drafter/manager wrap-as-draftset-owner]}]
  (-> (fn [{:keys [params] :as request}]
        (let [user-id (req/user-id request)
              {:keys [draftset-id metadata]} params
              source (ds-data-common/get-request-statement-source request)
              delete-job (delete-data manager user-id draftset-id source metadata)]
          (async-response/submitted-job-response delete-job)))
      inflate-gzipped
      temp-file-body
      deset-middleware/parse-graph-for-triples
      require-rdf-content-type
      wrap-as-draftset-owner))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.delete/delete-data-handler [_]
  (s/keys :req [:drafter/manager]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete/delete-data-handler [_ opts]
  (delete-draftset-data-handler opts))
