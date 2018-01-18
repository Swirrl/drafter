(ns drafter.feature.common
  (:require [drafter.rdf.draftset-management.job-util :as ajobs :refer [make-job]]
            [drafter.responses :as response]))

(defn- as-sync-write-job [f]
  (make-job
   :blocking-write [job]
    (let [result (f)]
      (ajobs/job-succeeded! job result))))

(defn run-sync
  ([api-call-fn]
   (response/run-sync-job! (as-sync-write-job api-call-fn)))
  ([api-call-fn resp-fn]
   (response/run-sync-job! (as-sync-write-job api-call-fn)
                           resp-fn)))
