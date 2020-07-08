(ns drafter.feature.draftset-data.delete
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.common :refer [->sesame-repo]]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as ops]
            [drafter.backend.draftset.rewrite-result :refer [rewrite-statement]]
            [drafter.draftset :as ds]
            [drafter.feature.draftset-data.common
             :as
             ds-data-common
             :refer
             [touch-graph-in-draftset!]]
            [drafter.feature.draftset-data.middleware :as deset-middleware]
            [drafter.middleware :refer [require-rdf-content-type temp-file-body inflate-gzipped]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sesame :refer [is-quads-format? read-statements]]
            [drafter.responses :as response]
            [drafter.util :as util]
            [drafter.write-scheduler :as writes]
            [grafter-2.rdf.protocols :refer [context]]
            [grafter-2.rdf4j.io :refer [quad->backend-quad]]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [drafter.async.jobs :as ajobs]
            [drafter.requests :as req])
  (:import org.eclipse.rdf4j.model.Resource))

(defn- delete-quads-from-draftset
  [resources quad-batches draftset-ref live->draft {:keys [op job-started-at] :as state} job]
  (let [repo (-> resources :backend :repo)]
    (case op
      :delete
      (if-let [batch (first quad-batches)]
        (let [live-graph (context (first batch))]
          (if (mgmt/is-graph-managed? repo live-graph)
            (if-let [draft-graph-uri (get live->draft live-graph)]
              (do
                (with-open [conn (repo/->connection (->sesame-repo repo))]
                  (touch-graph-in-draftset! conn draftset-ref draft-graph-uri job-started-at)
                  (let [rewritten-statements (map #(rewrite-statement live->draft %) batch)
                        sesame-statements (map quad->backend-quad rewritten-statements)
                        graph-array (into-array Resource (map util/uri->sesame-uri (vals live->draft)))]
                    (.remove conn sesame-statements graph-array)
                    (mgmt/unrewrite-draftset! conn {:draftset-uri (ds/->draftset-uri draftset-ref)
                                                    :deleted :rewrite})))
                (let [next-job (ajobs/create-child-job
                                job
                                (partial delete-quads-from-draftset resources (rest quad-batches) draftset-ref live->draft state))]
                  (writes/queue-job! next-job)))
              ;;NOTE: Do this immediately as we haven't done any real work yet
              (recur resources quad-batches draftset-ref live->draft (merge state {:op :copy-graph :live-graph live-graph}) job))
            ;;live graph does not exist so do not create a draft graph
            ;;NOTE: This is the same behaviour as deleting a live graph
            ;;which does not exist in live
            (recur resources (rest quad-batches) draftset-ref live->draft state job)))
        (let [draftset-info (ops/get-draftset-info repo draftset-ref)]
          (ajobs/job-succeeded! job {:draftset draftset-info})))

      :copy-graph
      (let [{:keys [live-graph]} state
            ds-uri (str (ds/->draftset-uri draftset-ref))
            {:keys [draft-graph-uri graph-map]} (mgmt/ensure-draft-exists-for repo live-graph live->draft ds-uri)]

        (ds-data-common/lock-writes-and-copy-graph resources live-graph draft-graph-uri {:silent true})
        ;; Now resume appending the batch
        (recur resources
               quad-batches
               draftset-ref
               (assoc live->draft live-graph draft-graph-uri)
               (merge state {:op :delete})
               job)))))

(defn batch-and-delete-quads-from-draftset [resources quads draftset-ref live->draft job clock-fn]
  (let [quad-batches (util/batch-partition-by quads context jobs/batched-write-size)
        now (clock-fn)]
    (delete-quads-from-draftset resources quad-batches draftset-ref live->draft {:op :delete :job-started-at now} job)))

(defn- get-quads [serialised {:keys [rdf-format graph]}]
  (let [statements (read-statements serialised rdf-format)]
    (if (is-quads-format? rdf-format)
      statements
      (map #(util/make-quad-statement % graph) statements))))

(defn delete-data-from-draftset-job
  [serialised user-id resources {:keys [draftset-id metadata] :as params} clock-fn]
  (let [repo (-> resources :backend :repo)]
    (jobs/make-job user-id
                   :background-write
                   (jobs/job-metadata repo draftset-id 'delete-data-from-draftset metadata)
                   (fn [job]
                     (let [quads (get-quads serialised params)
                           graph-mapping (ops/get-draftset-graph-mapping repo draftset-id)]
                       (batch-and-delete-quads-from-draftset resources
                                                             quads
                                                             draftset-id
                                                             graph-mapping
                                                             job
                                                             clock-fn))))))

(defn delete-draftset-data-handler
  [{:keys [wrap-as-draftset-owner :drafter/backend :drafter/global-writes-lock]}]
  (let [resources {:backend backend :global-writes-lock global-writes-lock}]
    (-> (fn [{:keys [params body] :as request}]
          (let [user-id (req/user-id request)
                delete-job (delete-data-from-draftset-job body user-id resources params util/get-current-time)]
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
