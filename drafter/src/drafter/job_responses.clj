(ns drafter.job-responses
  (:require
   [clojure.tools.logging :as log]
   [drafter.async.jobs :as jobs]
   [drafter.async.responses :as r :refer [submitted-job-response]]
   [drafter.rdf.draftset-management.job-util :refer [failed-job-result?]]
   [drafter.write-scheduler :as writes :refer [exec-sync-job!]]))

;; TODO should this namespace be merged with drafter.async.responses?

(defn- default-job-result-handler
  "Default handler for creating ring responses from job results. If
  the job succeeded then a 200 response is returned, otherwise a 500
  response."
  [result]
  (if (failed-job-result? result)
    (r/api-response 500 result)
    (r/api-response 200 result)))

;; run-sync-job! :: WriteLock -> Job -> RingResponse
;; run-sync-job! :: WriteLock -> Job -> (ApiResponse -> T) -> T
(defn run-sync-job!
  "Runs a sync job, blocks waiting for it to complete and returns a
  ring response using the given handler function. The handler function
  is passed the result of the job and should return a corresponding
  ring result map. If no handler is provided, the default job handler
  is used. If the job could not be queued, then a 503 'unavailable'
  response is returned."
  ([global-writes-lock job]
   (run-sync-job! global-writes-lock job default-job-result-handler))
  ([global-writes-lock job resp-fn]
   (log/info "Submitting sync job: " job)
   (try
     (let [job-result (exec-sync-job! global-writes-lock job)]
       (resp-fn job-result)))))

(defn enqueue-async-job!
  "Submits an async job for execution. Returns the submitted job."
  [job]
  (log/info "Submitting async job: " job)
  (writes/queue-job! job)
  (jobs/submit-async-job! job)
  job)

;; submit-async-job! :: Job -> RingResponse
(defn submit-async-job!
  "Submits an async job and returns a ring response indicating the
  result of the submit operation."
  [job]
  (enqueue-async-job! job)
  (submitted-job-response job))
