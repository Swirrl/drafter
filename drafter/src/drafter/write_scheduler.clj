(ns drafter.write-scheduler
  "This namespace implements a priority job queue of pending writes, and is
  responsible for ensuring writes are linearised without synchronous operations
  blocking for too long.

  Long-running synchronous operations can be scheduled as :publish-writes
  meaning any other concurrent write attempts will fail fast, rather block.

  The public functions in this namespace are concerned with submitting jobs and
  waiting for their results.

  Jobs can be added to the write queue using the queue-job! function."
  (:require
   [clojure.tools.logging :as log]
   [cognician.dogstatsd :as datadog]
   [drafter.async.jobs :as jobs]
   [drafter.logging :refer [with-logging-context]]
   [drafter.rdf.draftset-management.job-util :refer [failed-job-result?]]
   [drafter.responses :as r]
   [drafter.util :refer [log-time-taken]]
   [integrant.core :as ig])
  (:import [java.util.concurrent PriorityBlockingQueue TimeUnit]
           java.util.concurrent.atomic.AtomicBoolean
           java.util.concurrent.locks.ReentrantLock
           org.apache.log4j.MDC
           [java.util Date]))

(def priority-levels-map { :publish-write 2 :background-write 1})

(defonce write-scheduler-admin
         (atom {:jobs-flushed? nil :reject-mode? false}))

(def compare-jobs (comparator
                   (fn [job1 job2]
                     (let [ordering priority-levels-map
                           {type1 :priority time1 :start-time} job1
                           {type2 :priority time2 :start-time} job2]

                       (= -1 (compare [(ordering type1) time1]
                                      [(ordering type2) time2]))))))

(def timeunit
  {:days TimeUnit/DAYS
   :hours TimeUnit/DAYS
   :milliseconds TimeUnit/MILLISECONDS
   :minutes TimeUnit/MINUTES
   ;; :μs TimeUnit/MICROSECONDS ; included for completeness
   ;; :ns TimeUnit/NANOSECONDS  ; we're never going to use this, but they're
   :seconds TimeUnit/SECONDS})

(defn create-writes-lock
  "Creates a write lock with the specified fairness and lock acquire timeout"
  ([] (create-writes-lock {:fairness true :time 10 :unit :seconds}))
  ([{:keys [fairness time unit]}]
   ;; Set `fairness` to true for a fair lock policy, false for unfair.  See
   ;; ReentrantLock javadocs for details.
   {:lock (ReentrantLock. fairness)
    :time time
    :unit (timeunit unit)}))

(defmethod ig/init-key :drafter/global-writes-lock
  [_ opts]
  (create-writes-lock opts))

(defonce ^:private writes-queue (PriorityBlockingQueue. 11 compare-jobs))

(defmacro with-lock
  "Macro for executing forms inside a lock.  Takes a keyword for
  logging purposes and executes the supplied forms inside a lock.
  Requests to take the lock will wait forever for the lock to be
  released.  After the forms have executed the lock is guaranteed to
  be released."
  [global-writes-lock operation-type & forms]
  `(let [start-time# (System/currentTimeMillis)]
     (log/debug "Locking for" ~operation-type)
     (.lock (:lock ~global-writes-lock))
     (try
       (log/info "Acquired lock for " ~operation-type)
       ~@forms
       (finally
         (let [end-time# (System/currentTimeMillis)]
           (datadog/histogram! "drafter.writes_locked" (- end-time# start-time#)))
         (log/info "Releasing lock for" ~operation-type)
         (.unlock (:lock ~global-writes-lock))))))

(defn check-for-reject-mode []
  (when (:reject-mode? @write-scheduler-admin)
      (throw
        (ex-info (str "Write operations are temporarily unavailable due to maintenance. "
                      "Please try again later.")
                 {:error :writes-temporarily-disabled}))))

;;queue-job :: Job -> ()
(defn queue-job!
  "Adds a write job to the job queue. If the job cannot be queued then
  an ExceptionInfo is thrown with a data map containing a :type key
  mapped to a :job-enqueue-failed value."
  [{:keys [priority] :as job}]
  (check-for-reject-mode)
  (let [req-id (MDC/get "reqId")
        req-method (MDC/get "method")
        req-route (MDC/get "route")
        job (if req-id
              (with-meta job {:reqId req-id
                              :method req-method
                              :route req-route})
              job)]
    (log/info "Queueing job: " job (meta job))
    ;; Record the inc value since we can't do an atomic add+size op
    (datadog/gauge! "drafter.jobs_queue_size" (inc (.size writes-queue)))
    (.add writes-queue job)))

(defn exec-sync-job!
  "Executes a sync job waits for it to complete. Returns the result of
  the job execution.  Sync jobs skip the queue entirely and just run
  on the calling thread.  They do however check the writes-lock and
  will 503 if it's locked."
  [{:keys [lock time unit] :as opts}
   {job-function :function :keys [priority] :as job}]
  {:pre [(= :blocking-write priority)]}
  (check-for-reject-mode)

  ;; Try the lock & wait up to 10 seconds for it to let us in.  If it
  ;; doesn't raise an error.  This ensures :blocking-write operations
  ;; pass/fail within an acceptable time, and that only one writer is
  ;; writing to the database at a time.
  (if (.tryLock lock)
    ;; if we get the lock run the job immediately
    (try
      (job-function job)
      (finally
        (.unlock lock)))
    (throw
     (ex-info "Write operations are temporarily unavailable, due to other
               large write operations.  Please try again later."
              {:error :writes-temporarily-disabled}))))

(defn- accept-and-process-jobs! []
  (swap! write-scheduler-admin assoc
         :jobs-flushed? nil
         :reject-mode? false))

(defn jobs-flushing-check []
  (when (and
          (:reject-mode? @write-scheduler-admin)
          (:jobs-flushed? @write-scheduler-admin)
          (not (realized? (:jobs-flushed? @write-scheduler-admin)))
          (<= (.size writes-queue) 0))
    (deliver (:jobs-flushed? @write-scheduler-admin) true)))

(defn- reject-jobs-begin-flush! []
  (let [rejecting-msg (str "REJECTING: Writes and jobs are now being rejected at: " (Date.)
                           ". Waiting for jobs to flush...")
        flushed-msg "FLUSHED: write jobs have been flushed. It is now safe to take backups"]
    (swap! write-scheduler-admin assoc
           :reject-mode? true
           :jobs-flushed? (promise))
    ;; println message for socket toggle caller
    (println rejecting-msg)
    (log/warn rejecting-msg)
    (datadog/increment! "drafter.writes.entered_reject_mode" 1)
    ;; block until queued jobs are all processed and confirmed flushed
    @(:jobs-flushed? @write-scheduler-admin)
    (println flushed-msg)
    (log/warn flushed-msg)
    (datadog/increment! "drafter.writes.jobs_flushed" 1)))

(defn toggle-reject-and-flush!
  "This function is intended to be called from outside Drafter via a socket repl.
  It toggles Drafter into reject mode, where drafter rejects any new write jobs
  and waits for all currently running jobs to flush."
  []
  (if (:reject-mode? @write-scheduler-admin)
    (accept-and-process-jobs!)
    (reject-jobs-begin-flush!)))

(defn- write-loop
  "Start the write loop running.  Note this function does not return
  and is supposed to be run asynchronously on a future or thread.

  Users should normally use start-writer! to set this running."
  [global-writes-lock should-continue?]
  (log/debug "Writer started waiting for tasks")
  (loop []
    (when (.get should-continue?)
      (jobs-flushing-check)

      (when-let [{task-f! :function
                  priority :priority
                  job-id :id :as job} (.poll writes-queue 200 TimeUnit/MILLISECONDS)]
        (datadog/gauge! "drafter.jobs_queue_size" (.size writes-queue))
        (with-logging-context
          (assoc
            (meta job)
            :jobId (str (.substring (str job-id) 0 8)))
          (try
            ;; Note that task functions are responsible for the delivery
            ;; of the promise and the setting of DONE and also preserve
            ;; their job id.

            (log-time-taken "task"
                            (if (= :publish-write priority)
                              ;; If we're a publish operation we take the lock to
                              ;; ensure nobody else can write to the database.
                              (with-lock global-writes-lock :publish-write
                                         (task-f! job))
                              (task-f! job)))

            (catch Throwable ex
              (log/warn ex "A task raised an error.  Delivering error to promise")
              ;; TODO improve error returned
              (jobs/job-failed! job ex)))))
      (log/trace "Writer waiting for tasks")
      (recur))))

(defn start-writer! [global-writes-lock]
  (let [flag (AtomicBoolean. true)
        ^Runnable writer #(write-loop global-writes-lock flag)
        t (Thread. writer "Drafter write-loop thread")]
    (.start t)
    {:should-continue flag :thread t}))

(defn stop-writer! [{:keys [should-continue thread]}]
  (.set should-continue false)
  (.join thread))

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
   (check-for-reject-mode)
   (log/info "Submitting sync job: " job)
   (let [job-result (exec-sync-job! global-writes-lock job)]
     (resp-fn job-result))))

(defn enqueue-async-job!
  "Submits an async job for execution. Returns the submitted job."
  [job]
  (check-for-reject-mode)
  (log/info "Submitting async job: " job)
  (queue-job! job)
  (jobs/submit-async-job! job)
  job)

;; submit-async-job! :: Job -> RingResponse
(defn submit-async-job!
  "Submits an async job and returns a ring response indicating the
  result of the submit operation."
  [job]
  (check-for-reject-mode)
  (enqueue-async-job! job)
  (r/submitted-job-response job))

(defmethod ig/init-key :drafter/write-scheduler [_ opts]
  (start-writer! (:drafter/global-writes-lock opts)))

(defmethod ig/halt-key! :drafter/write-scheduler [_ writer]
  (stop-writer! writer))
