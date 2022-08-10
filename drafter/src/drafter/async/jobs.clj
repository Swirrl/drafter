(ns drafter.async.jobs
  (:require
   [clj-time.coerce :refer [from-long]]
   [clj-time.format :refer [formatters unparse]]
   [clojure.spec.alpha :as s]
   [cognician.dogstatsd :as datadog]
   [compojure.core :refer [context GET routes]]
   [drafter.async.spec :as spec]
   [drafter.draftset :as ds]
   [drafter.logging :refer [with-logging-context]]
   [drafter.middleware :as middleware]
   [drafter.responses :as r]
   [drafter.util :as util]
   [integrant.core :as ig])
  (:import clojure.lang.ExceptionInfo
           java.util.UUID
           org.apache.log4j.MDC
           [clojure.lang IDeref IBlockingDeref IPersistentMap]))

(defrecord Job [id
                user-id
                status
                priority
                start-time
                finish-time
                draftset-id
                draft-graph-id
                metadata
                function
                value-p]
  IDeref
  (deref [_] (deref value-p))

  IBlockingDeref
  (deref [_ timeout-ms timeout-val] (deref value-p timeout-ms timeout-val)))

(defmethod print-method Job [job writer]
  (let [m (get-method print-method IPersistentMap)]
    (m job writer)))

(def allowed-queue-keys
  #{:id :user-id :status :priority :start-time :finish-time
    :draftset-id :draft-graph-id :metadata :value-p})

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
      (select-keys [:id :user-id :status :priority :start-time
                    :finish-time :draftset-id :draft-graph-id :metadata])
      (update :start-time timestamp-response)
      (update :finish-time timestamp-response)
      (cond-> (and (= :complete (:status job))
                   (= :error (:type @value-p)))
        (assoc :error @value-p))))

(defmethod ig/init-key :drafter.routes/jobs-status [_ opts]
  (context
   "/v1/status" []
   (middleware/wrap-authorize (:wrap-authenticate opts) :drafter:job:view
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
      (with-logging-context {:jobId (str "job-" (.substring (str job-id) 0 8))
                                 :reqId request-id}
        (f job)))))


(s/fdef create-job
  :args (s/cat :user-id ::spec/user-id
               :metadata ::spec/metadata
               :priority ::spec/priority
               :f ::spec/function)
  :ret ::spec/job)

(defn create-job
  [user-id metadata priority f]
  (let [id (UUID/randomUUID)]
    (->Job id
           user-id
           :pending
           priority
           (System/currentTimeMillis)
           nil
           (when-let [id (-> metadata :draftset :id)] (ds/->DraftsetId id))
           nil
           metadata
           (wrap-logging-context f)
           (promise))))

(defn submit-async-job!
  "Submits an async job and returns `true` if the job was submitted
  successfully"
  [job]
  (let [job' (select-keys job allowed-queue-keys)]
    (swap! jobs assoc-in [:pending (:id job')] job'))
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
  (let [job' (-> job
                 (assoc :status :complete
                        :finish-time (System/currentTimeMillis))
                 (dissoc :function) ;; remove function closure as otherwise it will leak batches of quads
                 (map->Job)
                 )]
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

(defn- record-job-stats!
  "Log a job completion to datadog"
  [job suffix]
  (datadog/increment! (util/statsd-name "drafter.job"
                                        (-> job :metadata :operation)
                                        (:priority job)
                                        suffix)
                      1))

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
   (record-job-stats! job :failed)
   (complete-job! job (failed-job-result ex details))))

(defn job-succeeded!
  "Complete's the job with complete-job! and sets it's response :type
  as \"ok\" indicating that it completed without error.  If a details
  value is provided it will be added to the job result map under
  the :details key."
  ([job]
   {:pre [(not (job-completed? job))]
    :post [(job-completed? job)]}
   (record-job-stats! job :succeeded)
   (complete-job! job {:type :ok}))
  ([job details]
   {:pre [(not (job-completed? job))]
    :post [(job-completed? job)]}
   (record-job-stats! job :succeeded)
   (complete-job! job {:type :ok :details details})))
