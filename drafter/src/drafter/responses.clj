(ns drafter.responses
  (:require [clojure.string :refer [upper-case]]
            [clojure.tools.logging :as log]
            [drafter.rdf.draftset-management.job-util :refer [failed-job-result?]]
            [drafter.write-scheduler :as writes :refer [exec-sync-job!]]
            [drafter.async.jobs :as jobs]
            [drafter.async.responses :as r :refer [submitted-job-response]]
            [swirrl-server.errors :refer [encode-error]]))

(defmethod encode-error :writes-temporarily-disabled [ex]
  (r/error-response 503 ex))

(defmethod encode-error :payload-too-large [ex]
  (r/error-response 413 ex))

(defmethod encode-error :unprocessable-request [ex]
  (r/error-response 413 ex))

(defn not-acceptable-response
  ([] (not-acceptable-response ""))
  ([body] {:status 406 :headers {} :body body}))

(defn unprocessable-entity-response [body]
  {:status 422 :headers {} :body body})

(defn unsupported-media-type-response [body]
  {:status 415 :headers {} :body body})

(defn method-not-allowed-response [method]
  {:status 405
   :headers {}
   :body (str "Method " (upper-case (name method)) " not supported by this resource")})

(defn unauthorised-basic-response [realm]
  (let [params (str "Basic realm=\"" realm "\"")]
    {:status 401 :body "" :headers {"WWW-Authenticate" params}}))

(defn forbidden-response [body]
  {:status 403 :body body :headers {}})

(defn conflict-detected-response [body]
  {:status 409 :body body :headers {}})

(defn default-job-result-handler
  "Default handler for creating ring responses from job results. If
  the job succeeded then a 200 response is returned, otherwise a 500
  response."
  [result]
  (if (failed-job-result? result)
    (r/api-response 500 result)
    (r/api-response 200 result)))

;; run-sync-job! :: WriteLock -> Job -> RingResponse
;; run-sync-job! :: WriteLock -> Job -> (ApiResponse -> RingResponse) -> RingResponse
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

;; submit-async-job! :: Job -> RingResponse
(defn submit-async-job!
  "Submits an async job and returns a ring response indiciating the
  result of the submit operation."
  [job]
  (log/info "Submitting async job: " job)
  (writes/queue-job! job)
  (jobs/submit-async-job! job)
  (submitted-job-response "/v1" job))
