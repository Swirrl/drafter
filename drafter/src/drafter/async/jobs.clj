(ns drafter.async.jobs
  (:require [compojure.core :refer [context GET routes]]
            [clojure.spec.alpha :as s]
            [clj-logging-config.log4j :as l4j]
            [clojure.tools.logging :as log]
            [drafter.async.responses :as r]
            [drafter.async.spec :as spec]
            [integrant.core :as ig]
            [clj-time.coerce :refer [to-date from-long]]
            [clj-time.core :as time]
            [clj-time.format :refer [formatters unparse]])
  (:import (java.util UUID)
           (clojure.lang ExceptionInfo)
           (org.apache.log4j MDC)))

(defrecord Job [id
                user-id
                operation
                status
                priority
                start-time
                finish-time
                draftset-id
                draft-graph-id
                metadata
                function
                value-p])

(defonce jobs
  (atom {:pending  {}
         :complete {}}))

(defn complete-job [id]
  (some-> jobs deref :complete (get id)))

(defn get-job [id]
  (or (some-> jobs deref :pending (get id))
      (complete-job id)))

(def not-found
  {:status 404
   :headers {"Content-Type" "text/plain"}
   :body "Job not found."})

(defn timestamp-response [x]
  (some->> x from-long (unparse (formatters :date-time))))

(s/fdef job-response :args (s/cat :job ::spec/job) :ret ::spec/api-job)

(defn job-response [{:keys [value-p] :as job}]
  (-> job
      (select-keys [:id :user-id :operation :status :priority :start-time
                    :finish-time :draftset-id :draft-graph-id :metadata])
      (update :start-time timestamp-response)
      (update :finish-time timestamp-response)
      (cond-> (and (= :complete (:status job))
                   (= :error (:type @value-p)))
        (assoc :error @value-p))))

(defmethod ig/init-key :drafter.routes/jobs-status [_ {:keys [wrap-auth]}]
  (context
   "/v1/status" []
   (wrap-auth
    (routes
     (GET "/jobs/:id" [id]
          (or (when-let [job (some-> id r/try-parse-uuid get-job)]
                (r/json-response 200 (job-response job)))
              not-found))
     (GET "/jobs" []
          (r/json-response 200 (mapv job-response
                                     (concat
                                      (-> jobs deref :pending vals)
                                      (-> jobs deref :complete vals)))))
     (GET "/finished-jobs/:id" [id]
          (or (when-let [job (some-> id r/try-parse-uuid complete-job :value-p deref)]
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
  :args (s/or :ary-4 (s/cat :user-id ::spec/user-id
                            :operation ::spec/operation
                            :priority ::spec/priority
                            :f ::spec/function)
              :ary-5 (s/cat :user-id ::spec/user-id
                            :operation ::spec/operation
                            :draftset-id (s/nilable ::spec/draftset-id)
                            :priority ::spec/priority
                            :f ::spec/function)
              :ary-6 (s/cat :user-id ::spec/user-id
                            :operation ::spec/operation
                            :draftset-id (s/nilable ::spec/draftset-id)
                            :metadata ::spec/metadata
                            :priority ::spec/priority
                            :f ::spec/function))
  :ret ::spec/job)

(defn create-job
  ([user-id operation priority f]
   (create-job user-id operation nil priority f))
  ([user-id operation draftset-id priority f]
   (create-job user-id operation draftset-id nil priority f))
  ([user-id operation draftset-id metadata priority f]
   (let [id (UUID/randomUUID)]
     (->Job id
            user-id
            operation
            :pending
            priority
            (System/currentTimeMillis)
            nil
            draftset-id
            nil
            metadata
            (wrap-logging-context f)
            (promise)))))

(defn submit-async-job!
  "Submits an async job and returns `true` if the job was submitted
  successfully"
  [job]
  (swap! jobs assoc-in [:pending (:id job)] job)
  true)

(defn job-completed?
  "Whether the given job has been completed"
  [job]
  (realized? (:value-p job)))

(defn create-child-job
  "Creates a continuation job from the given parent."
  [job child-fn]
  (assoc job :function child-fn :start-time (System/currentTimeMillis)))

(defn- job-pending? [job]
  (-> jobs deref :pending (contains? (:id job))))

(defn- complete-pending-job!
  "Move a job in the pending list into the complete list, adding :complete
  and :finish-time metadata."
  [{job-id :id :as job}]
  (let [job' (assoc job
                    :status :complete
                    :finish-time (System/currentTimeMillis))]
    (swap! jobs (fn [jobs]
                  (-> jobs
                      (update :pending dissoc job-id)
                      (update :complete assoc job-id job'))))))

(defn- complete-job!
  "Adds the job to the state map of finished-jobs and delivers the
  supplied result to the jobs promise, which will cause blocking jobs
  to unblock, and give job consumers the ability to receive the
  value."
  [{promis :value-p :as job} result]
  (deliver promis result)
  (when (job-pending? job)
    (complete-pending-job! job))
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
