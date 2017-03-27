(ns drafter.write-scheduler
  "This namespace implements a priority job queue of pending writes, and is
  responsible for ensuring writes are linearised without synchronous operations
  blocking for too long.

  Long running synchronous operations can be scheduled as :publish-writes
  meaning any other concurrent write attempts will fail fast, rather block.

  The public functions in this namespace are concerned with submitting jobs and
  waiting for their results.

  Jobs can be added to the write queue using the queue-job! function."
  (:require [clojure.tools.logging :as log]
            [clj-logging-config.log4j :as l4j]
            [drafter.util :refer [log-time-taken]]
            [swirrl-server.async.jobs :refer [finished-jobs job-failed! restart-id ->Job]]
            [swirrl-server.errors :refer [ex-swirrl encode-error]])
  (:import (org.apache.log4j MDC)
           (java.util.concurrent PriorityBlockingQueue TimeUnit)
           (java.util.concurrent.locks ReentrantLock)))

(def priority-levels-map { :publish-write 2 :background-write 1})

(def compare-jobs (comparator
                   (fn [job1 job2]
                     (let [ordering priority-levels-map
                           {type1 :priority time1 :time} job1
                           {type2 :priority time2 :time} job2]

                       (= -1 (compare [(ordering type1) time1]
                                      [(ordering type2) time2]))))))

(def fairness
  "Set to true for a fair lock policy, false for unfair.  See
  ReentrantLock javadocs for details."
  true)

(def global-writes-lock (ReentrantLock. fairness))

(defonce ^:private writes-queue (PriorityBlockingQueue. 11 compare-jobs))

(defmacro with-lock
  "Macro for executing forms inside a lock.  Takes a keyword for
  logging purposes and executes the supplied forms inside a lock.
  Requests to take the lock will wait forever for the lock to be
  released.  After the forms have executed the lock is guaranteed to
  be released."
  [operation-type & forms]
  `(do
     (log/debug "Locking for" ~operation-type)
     (.lock global-writes-lock)
     (try
       (log/info "Acquired lock for " ~operation-type)
       ~@forms
       (finally
         (log/info "Releasing lock for" ~operation-type)
         (.unlock global-writes-lock)))))

;;queue-job :: Job -> ()
(defn queue-job!
  "Adds a write job to the job queue. If the job cannot be queued then
  an ExceptionInfo is thrown with a data map containing a :type key
  mapped to a :job-enqueue-failed value."
  [job]
  (let [req-id (MDC/get "reqId")
        req-method (MDC/get "method")
        req-route (MDC/get "route")
        job (if req-id
              (with-meta job {:reqId req-id
                              :method req-method
                              :route req-route})
              job)]
    (log/info "Queueing job: " job (meta job))
    (.add writes-queue job)))

;;await-sync-job! :: Job -> ApiResponse
(defn exec-sync-job!
  "Executes a sync job waits for it to complete. Returns the result of
  the job execution.  Sync jobs skip the queue entirely and just run
  on the calling thread.  They do however check the writes-lock and
  will 503 if it's locked."
  [{job-function :function :keys [value-p priority] :as job}]
  {:pre [(= :blocking-write priority)]}

  ;; Try the lock & wait up to 10 seconds for it to let us in.  If it
  ;; doesn't raise an error.  This ensure's :blocking-write's
  ;; pass/fail within an acceptable time, and that only one writer is
  ;; writing to the database at a time.
  (if (.tryLock global-writes-lock 10 TimeUnit/SECONDS)
    ;; if we get the lock run the job immediately
    (try
      (job-function job)
      (finally
        (.unlock global-writes-lock)))
    (throw (ex-swirrl :writes-temporarily-disabled "Write operations are temporarily unavailable, due to other large write operations.  Please try again later."))))

(defn- write-loop
  "Start the write loop running.  Note this function does not return
  and is supposed to be run asynchronously on a future or thread.

  Users should normally use start-writer! to set this running."

  []
  (log/debug "Writer started waiting for tasks")
    (loop [{task-f! :function
            priority :priority
            job-id :id
            promis :value-p :as job} (.take writes-queue)]
      (l4j/with-logging-context (assoc
                                 (meta job)
                                 :jobId (str "job-" (.substring (str job-id) 0 8)))
        (try
          ;; Note that task functions are responsible for the delivery
          ;; of the promise and the setting of DONE and also preserve
          ;; their job id.

          (log-time-taken "task"
            (if (= :publish-write priority)
              ;; If we're a publish operation we take the lock to
              ;; ensure nobody else can write to the database.
              (with-lock :publish-write
                (task-f! job))
              (task-f! job)))

          (catch Exception ex
            (log/warn ex "A task raised an error.  Delivering error to promise")
            ;; TODO improve error returned
            (job-failed! job ex))))
      (log/debug "Writer waiting for tasks")
      (recur (.take writes-queue))))

(defn start-writer! []
  (future
    (write-loop)))

(defn stop-writer! [writer]
  (future-cancel writer))
