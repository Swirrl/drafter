(ns drafter.feature.common
  (:require [drafter.backend.draftset.operations :as dsops]
            [drafter.rdf.draftset-management.job-util :as jobutil]
            [drafter.responses :as response]
            [ring.util.response :as ring]
            [drafter.async.responses :as r]
            [drafter.async.jobs :as ajobs]))

(defn draftset-sync-write-response [result backend draftset-id]
  (if (jobutil/failed-job-result? result)
    (r/api-response 500 result)
    (ring/response (dsops/get-draftset-info backend draftset-id))))

(defn- as-sync-write-job [user-id draftset-id f]
  (jobutil/make-job
   user-id draftset-id
   :blocking-write [job]
    (let [result (f)]
      (ajobs/job-succeeded! job result))))

(defn run-sync
  ([user-id draftset-id api-call-fn]
   (response/run-sync-job! (as-sync-write-job user-id draftset-id api-call-fn)))
  ([user-id draftset-id api-call-fn resp-fn]
   (response/run-sync-job! (as-sync-write-job user-id draftset-id api-call-fn)
                           resp-fn)))
