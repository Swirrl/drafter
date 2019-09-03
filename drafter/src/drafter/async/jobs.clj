(ns drafter.async.jobs
  (:require [compojure.core :refer [context GET routes]]
            [clojure.spec.alpha :as s]
            [clj-logging-config.log4j :as l4j]
            [clojure.tools.logging :as log]
            [drafter.async.responses :as r]
            [drafter.async.spec :as spec]
            [swirrl-server.util :refer [try-parse-uuid]]
            [integrant.core :as ig]
            [clj-time.coerce :refer [to-date from-long]]
            [clj-time.core :as time]
            [clj-time.format :refer [formatters unparse]])
  (:import (java.util UUID)
           (clojure.lang ExceptionInfo)
           (org.apache.log4j MDC)))

;; old job (defrecord Job [id priority time function value-p])

;; TODO: will a job always have a user? a draftset? a graph? a name?
(defrecord Job [id
                user-id
                status
                priority
                start-time
                finish-time
                draftset-id
                draft-graph-id
                function
                value-p])

(defonce jobs
  {:pending  (ref {})
   :complete (ref {})})

(defn complete-job [id]
  (some-> jobs :complete deref (get id)))

(defn get-job [id]
  (or (some-> jobs :pending  deref (get id))
      (complete-job id)))

(def not-found
  {:status 404
   :headers {"Content-Type" "text/plain"}
   :body "Job not found."})

(defn timestamp-response [x]
  (some->> x from-long (unparse (formatters :date-time))))

(s/fdef job-response :args (s/cat :job ::spec/job) :ret ::spec/api-job)

(defn job-response [job]
  (-> job
      (select-keys [:id :user-id :status :priority :start-time :finish-time
                    :draftset-id :draft-graph-id])
      (update :start-time timestamp-response)
      (update :finish-time timestamp-response)))

(defmethod ig/init-key :drafter.routes/jobs-status [_ {:keys [wrap-auth]}]
  (context
   "/v1/status" []
   (wrap-auth
    (routes
     (GET "/jobs/:id" [id]
          (or (when-let [job (some-> id try-parse-uuid get-job)]
                (r/json-response 200 (job-response job)))
              not-found))
     (GET "/jobs" []
          (r/json-response 200 (mapv job-response
                                     (concat
                                      (-> jobs :pending deref vals)
                                      (-> jobs :complete deref vals)))))
     (GET "/finished-jobs/:id" [id]
          (or (when-let [job (some-> id try-parse-uuid complete-job :value-p deref)]
                (r/json-response 200 (assoc job :restart-id r/restart-id)))
              (r/job-not-finished-response r/restart-id)))))))

(defn- wrap-logging-context
  "Preserve the jobId and requestId in log4j logs."
  [f]
  (let [request-id (MDC/get "reqId")] ;; copy reqId off calling thread
    (fn [{job-id :id :as job}]
      (l4j/with-logging-context {:jobId (str "job-" (.substring (str job-id) 0 8))
                                 :reqId request-id}
        (f job)))))


(s/fdef create-job
  :args (s/or :ary-3 (s/cat :user-id ::spec/user-id
                            :priority ::spec/priority
                            :f ::spec/function)
              :ary-4 (s/cat :user-id ::spec/user-id
                            :draftset-id (s/nilable ::spec/draftset-id)
                            :priority ::spec/priority
                            :f ::spec/function))
  :ret ::spec/job)

(defn create-job
  ;;  create-job : UUID -> Priority -> JobFn -> Job
  ([user-id priority f]
   (create-job user-id nil priority f))
  ;;  create-job : UUID -> UUID -> Priority -> JobFn -> Job
  ([user-id draftset-id priority f]
   (let [id (UUID/randomUUID)]
     (->Job id
            user-id
            :pending
            priority
            (System/currentTimeMillis)
            nil
            draftset-id
            nil
            (wrap-logging-context f)
            (promise)))))

;; submit-async-job! : Job -> IO Boolean
(defn submit-async-job!
  "Submits an async job and returns `true` if the job was submitted
  successfully"
  [job]
  (dosync (alter (:pending jobs) assoc (:id job) job))
  true)

(defn job-completed?
  "Whether the given job has been completed"
  [job]
  (realized? (:value-p job)))

(defn create-child-job
  "Creates a continuation job from the given parent."
  [job child-fn]
  (assoc job :function child-fn :start-time (System/currentTimeMillis)))

(defn- complete-job!
  "Adds the job to the state map of finished-jobs and delivers the
  supplied result to the jobs promise, which will cause blocking jobs
  to unblock, and give job consumers the ability to receive the
  value."
  [{job-id :id promis :value-p :as job} result]
  (deliver promis result)
  (let [job' (assoc job
                    :status :complete
                    :finish-time (System/currentTimeMillis))]
    (dosync
     (alter (:pending jobs) dissoc job-id)
     (alter (:complete jobs) assoc job-id job')))
  result)

(defn- failed-job-result [ex details]
  (let [result {:type :error
                :message (.getMessage ex)
                :error-class (.getName (class ex))}]
    (if (some? details)
      (assoc result :details details)
      result)))

(defn job-failed!
  "Mark the given job as failed. If a details map is provided it will
  be associated with the job result under the :details key. If no map
  is provided and ex is an instance of ExceptionInfo the ex-data of
  the exception will be used as the details."
  ([job ex]
   (job-failed! job ex (when (instance? ExceptionInfo ex)
                        (ex-data ex))))
  ([job ex details]
   {:pre [(not (job-completed? job))]
    :post [(job-completed? job)]}
   (complete-job! job (failed-job-result ex details))))

(defn job-succeeded!
  "Complete's the job with complete-job! and sets it's response :type
  as \"ok\" indicating that it completed without error.  If a details
  value is provided it will be added to the job result map under
  the :details key."
  ([job]
   {:pre [(not (job-completed? job))]
    :post [(job-completed? job)]}
   (complete-job! job {:type :ok}))
  ([job details]
   {:pre [(not (job-completed? job))]
    :post [(job-completed? job)]}
   (complete-job! job {:type :ok :details details})))
