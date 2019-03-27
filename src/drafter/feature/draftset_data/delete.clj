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
            [drafter.middleware :refer [require-rdf-content-type temp-file-body]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sesame :refer [is-quads-format? read-statements]]
            [drafter.responses :as response]
            [drafter.util :as util]
            [drafter.write-scheduler :as writes]
            [grafter-2.rdf.protocols :refer [context]]
            [grafter-2.rdf4j.io :refer [quad->backend-quad]]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [swirrl-server.async.jobs :as ajobs])
  (:import org.eclipse.rdf4j.model.Resource))

(defn- delete-quads-from-draftset [backend quad-batches draftset-ref live->draft {:keys [op job-started-at] :as state} job]
  (case op
    :delete
    (if-let [batch (first quad-batches)]
      (let [live-graph (context (first batch))]
        (if (mgmt/is-graph-managed? backend live-graph)
          (if-let [draft-graph-uri (get live->draft live-graph)]
            (do
              (with-open [conn (repo/->connection (->sesame-repo backend))]
                (touch-graph-in-draftset! conn draftset-ref draft-graph-uri job-started-at)
                (let [rewritten-statements (map #(rewrite-statement live->draft %) batch)
                      sesame-statements (map quad->backend-quad rewritten-statements)
                      graph-array (into-array Resource (map util/uri->sesame-uri (vals live->draft)))]
                  (.remove conn sesame-statements graph-array)))
              (let [next-job (ajobs/create-child-job
                              job
                              (partial delete-quads-from-draftset backend (rest quad-batches) draftset-ref live->draft state))]
                (writes/queue-job! next-job)))
            ;;NOTE: Do this immediately as we haven't done any real work yet
            (recur backend quad-batches draftset-ref live->draft (merge state {:op :copy-graph :live-graph live-graph}) job))
          ;;live graph does not exist so do not create a draft graph
          ;;NOTE: This is the same behaviour as deleting a live graph
          ;;which does not exist in live
          (recur backend (rest quad-batches) draftset-ref live->draft state job)))
      (let [draftset-info (ops/get-draftset-info backend draftset-ref)]
        (ajobs/job-succeeded! job {:draftset draftset-info})))

    :copy-graph
    (let [{:keys [live-graph]} state
          ds-uri (str (ds/->draftset-uri draftset-ref))
          {:keys [draft-graph-uri graph-map]} (mgmt/ensure-draft-exists-for backend live-graph live->draft ds-uri)]

      (ds-data-common/lock-writes-and-copy-graph backend live-graph draft-graph-uri {:silent true})
      ;; Now resume appending the batch
      (recur backend
             quad-batches
             draftset-ref
             (assoc live->draft live-graph draft-graph-uri)
             (merge state {:op :delete})
             job))))

(defn batch-and-delete-quads-from-draftset [backend quads draftset-ref live->draft job clock-fn]
  (let [quad-batches (util/batch-partition-by quads context jobs/batched-write-size)
        now (clock-fn)]
    (delete-quads-from-draftset backend quad-batches draftset-ref live->draft {:op :delete :job-started-at now} job)))



(defn delete-quads-from-draftset-job [backend draftset-ref serialised rdf-format clock-fn]
  (let [backend (:repo backend)]
    (jobs/make-job :background-write [job]
                   (let [;;backend (ep/draftset-endpoint {:backend backend :draftset-ref draftset-ref :union-with-live? false})
                         quads (read-statements serialised rdf-format)
                         graph-mapping (ops/get-draftset-graph-mapping backend draftset-ref)]
                     (batch-and-delete-quads-from-draftset backend quads draftset-ref graph-mapping job clock-fn)))))

(defn delete-triples-from-draftset-job [backend draftset-ref graph serialised rdf-format clock-fn]
  (let [backend (:repo backend)]
    (jobs/make-job :background-write [job]
                   (let [;;backend (ep/draftset-endpoint {:backend backend :draftset-ref draftset-ref :union-with-live? false})
                         triples (read-statements serialised rdf-format)
                         quads (map #(util/make-quad-statement % graph) triples)
                         graph-mapping (ops/get-draftset-graph-mapping backend draftset-ref)]
                     (batch-and-delete-quads-from-draftset backend quads draftset-ref graph-mapping job clock-fn)))))


(defn delete-draftset-data-handler [{wrap-as-draftset-owner :wrap-as-draftset-owner backend :drafter/backend}]
  (wrap-as-draftset-owner
   (require-rdf-content-type
    (deset-middleware/require-graph-for-triples-rdf-format
     (temp-file-body
      (fn [{{draftset-id :draftset-id
            graph :graph
            rdf-format :rdf-format} :params body :body :as request}]
        (let [delete-job (if (is-quads-format? rdf-format)
                           (delete-quads-from-draftset-job backend draftset-id body rdf-format util/get-current-time)
                           (delete-triples-from-draftset-job backend draftset-id graph body rdf-format util/get-current-time))]
          (response/submit-async-job! delete-job))))))))

(defmethod ig/pre-init-spec ::delete-data-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::delete-data-handler [_ opts]
  (delete-draftset-data-handler opts))
