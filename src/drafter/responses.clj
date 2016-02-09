(ns drafter.responses
  (:require [clojure.tools.logging :as log]
            [swirrl-server.responses :as response]
            [drafter.rdf.draft-management.jobs :refer [failed-job-result?]]
            [swirrl-server.async.jobs :refer [submitted-job-response]]
            [drafter.write-scheduler :refer [await-sync-job! queue-job!]]
            [clojure.string :refer [upper-case]])
  (:import [clojure.lang ExceptionInfo]))

(def ^:private temporarily-locked-for-writes-response
  {:status 503 :body {:type :error :message "Write operations are temporarily unavailable.  Please try again later."}})

(defn unknown-rdf-content-type-response [content-type]
  (response/bad-request-response
   (str "Unknown RDF format for content type " content-type)))

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

(defn default-job-result-handler
  "Default handler for creating ring responses from job results. If
  the job succeeded then a 200 response is returned, otherwise a 500
  response."
  [result]
  (if (failed-job-result? result)
    (response/api-response 500 result)
    (response/api-response 200 result)))

;; submit-sync-job! :: Job -> RingResponse
;; submit-sync-job! :: Job -> (ApiResponse -> RingResponse) -> RingResponse
(defn submit-sync-job!
  "Submits a sync job, blocks waiting for it to complete and returns a
  ring response using the given handler function. The handler function
  is passed the result of the job and should return a corresponding
  ring result map. If no handler is provided, the default job handler
  is used. If the job could not be queued, then a 503 'unavailable'
  response is returned."
  ([job] (submit-sync-job! job default-job-result-handler))
  ([job resp-fn]
   (log/info "Submitting sync job: " job)
   (try
     (let [job-result (await-sync-job! job)]
       (resp-fn job-result))
     (catch ExceptionInfo ex temporarily-locked-for-writes-response))))

(defmacro handle-sync-job!
  "Convenience macro for submitting synchronous jobs and converting
  the results into ring responses. At least three forms are required -
  the first is for the job, the second for the job result and the
  remaining are used to construct the ring response. The job result
  form can be used to destructure the map used to complete the job.

  (handle-sync-job (create-job)
     {:keys [message] :as result} (create-response message))"

  [job result-form & response-forms]
  `(submit-sync-job! ~job (fn [~result-form] ~@response-forms)))

;; submit-async-job! :: Job -> RingResponse
(defn submit-async-job!
  "Submits an async job and returns a ring response indiciating the
  result of the submit operation."
  [job]
  (log/info "Submitting async job: " job)
  (try
    (queue-job! job)
    (submitted-job-response job)
    (catch ExceptionInfo ex temporarily-locked-for-writes-response)))

(defn is-client-error-response?
  "Whether the given ring response map represents a client error."
  [{:keys [status] :as response}]
  (and (>= status 400)
       (< status 500)))
