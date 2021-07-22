(ns drafter.feature.common
  (:require
   [drafter.async.jobs :as ajobs]
   [drafter.async.responses :as r]
   [drafter.backend.draftset.operations :as dsops]
   [drafter.rdf.draftset-management.job-util :as jobs]
   [drafter.responses :as response]
   [ring.util.response :as ring]))

(defn draftset-sync-write-response [result backend draftset-id]
  (if (jobs/failed-job-result? result)
    (r/api-response 500 result)
    (ring/response (dsops/get-draftset-info backend draftset-id))))

(defn- as-sync-write-job [backend user-id operation draftset-id f]
  (jobs/make-job user-id
                    :blocking-write
                    (jobs/job-metadata backend draftset-id operation nil)
                    (fn [job]
                      (let [result (f)]
                        (ajobs/job-succeeded! job result)))))

(defn run-sync
  ([{:keys [backend global-writes-lock]} user-id operation draftset-id api-call-fn]
   (response/run-sync-job! global-writes-lock
    (as-sync-write-job backend user-id operation draftset-id api-call-fn)))
  ([{:keys [backend global-writes-lock]} user-id operation draftset-id api-call-fn resp-fn]
   (response/run-sync-job! global-writes-lock
    (as-sync-write-job backend user-id operation draftset-id api-call-fn) resp-fn)))
