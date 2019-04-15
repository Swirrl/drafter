(ns drafter.feature.common
  (:require [drafter.backend.draftset.operations :as dsops]
            [drafter.rdf.draftset-management.job-util :as jobutil]
            [drafter.responses :as response]
            [ring.util.response :as ring]
            [swirrl-server.responses :as ss-response]
            [swirrl-server.async.jobs :as ajobs]))

(defn draftset-sync-write-response [result backend draftset-id]
  (if (jobutil/failed-job-result? result)
    (ss-response/api-response 500 result)
    (ring/response (dsops/get-draftset-info backend draftset-id))))

(defn- as-sync-write-job [f]
  (jobutil/make-job
   :blocking-write [job]
    (let [result (f)]
      (ajobs/job-succeeded! job result))))

(defn run-sync
  ([api-call-fn]
   (response/run-sync-job! (as-sync-write-job api-call-fn)))
  ([api-call-fn resp-fn]
   (response/run-sync-job! (as-sync-write-job api-call-fn)
                           resp-fn)))
