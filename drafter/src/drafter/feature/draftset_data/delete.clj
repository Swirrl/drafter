(ns drafter.feature.draftset-data.delete
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.graphs :as graphs]
            [drafter.backend.draftset.operations :as ops]
            [drafter.backend.draftset.rewrite-result :refer [rewrite-statement]]
            [drafter.draftset :as ds]
            [drafter.feature.draftset-data.common :as ds-data-common]
            [drafter.feature.draftset-data.middleware :as deset-middleware]
            [drafter.middleware :refer [require-rdf-content-type temp-file-body inflate-gzipped]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sesame :refer [is-quads-format? read-statements]]
            [drafter.responses :as response]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.io :as gio]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [drafter.requests :as req]
            [drafter.time :as time])
  (:import org.eclipse.rdf4j.model.Resource))

(defn- delete-quad-batch! [repo {:keys [draftset-ref job-started-at] :as context} live->draft draft-graph-uri batch]
  (with-open [conn (repo/->connection repo)]
    (ds-data-common/touch-graph-in-draftset! conn draftset-ref draft-graph-uri job-started-at)
    (let [rewritten-statements (map #(rewrite-statement live->draft %) batch)
          sesame-statements (map gio/quad->backend-quad rewritten-statements)
          graph-array (into-array Resource (map util/uri->sesame-uri (vals live->draft)))]
      (.remove conn sesame-statements graph-array)
      (mgmt/unrewrite-draftset! conn {:draftset-uri (ds/->draftset-uri draftset-ref)
                                      :deleted :rewrite}))))

(defn- done! [{:keys [draftset-ref] :as context}]
  (let [repo (ds-data-common/get-repo context)
        draftset-info (ops/get-draftset-info repo draftset-ref)]
    (ds-data-common/done-state {:draftset draftset-info})))

(defn- delete-state [{:keys [live->draft] :as state} context]
  (let [repo (ds-data-common/get-repo context)]
    (loop [quad-batches (:quad-batches state)]
      (if-let [batch (first quad-batches)]
        (let [live-graph (pr/context (first batch))]
          (if (mgmt/is-graph-managed? repo live-graph)
            (if-let [draft-graph-uri (get live->draft live-graph)]
              (do
                (delete-quad-batch! repo context live->draft draft-graph-uri batch)
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
  [source user-id resources draftset-id clock metadata]
  (let [repo (-> resources :backend :repo)
        job-meta (jobs/job-metadata repo draftset-id 'delete-data-from-draftset metadata)
        sm (delete-state-machine)]
    (ds-data-common/create-state-machine-job resources user-id draftset-id source clock job-meta sm)))

(defn delete-draftset-data-handler
  [{:keys [:drafter/backend :drafter/global-writes-lock
           ::graphs/manager
           ::time/clock wrap-as-draftset-owner]}]
  (let [resources {:backend backend
                   :global-writes-lock global-writes-lock
                   :graph-manager manager}]
    (-> (fn [{:keys [params] :as request}]
          (let [{:keys [draftset-id metadata]} params
                user-id (req/user-id request)
                source (ds-data-common/get-request-statement-source request)
                delete-job (delete-data-from-draftset-job source user-id resources draftset-id clock metadata)]
            (response/submit-async-job! delete-job)))
        inflate-gzipped
        temp-file-body
        deset-middleware/require-graph-for-triples-rdf-format
        require-rdf-content-type
        wrap-as-draftset-owner)))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.delete/delete-data-handler [_]
  (s/keys :req [:drafter/backend
                :drafter/global-writes-lock]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete/delete-data-handler [_ opts]
  (delete-draftset-data-handler opts))
