(ns drafter.write-scheduler
  "This namespace implements a priority job queue of pending writes, and is
  responsible for ensuring writes are linearised without synchronous operations
  blocking for too long.

  Long running synchronous operations can be scheduled as :exclusive-writes
  meaning any other concurrent write attempts will fail fast, rather block.

  The public functions in this namespace are concerned with submitting jobs and
  waiting for their results.

  Jobs can be added to the write queue using the queue-job! function."
  (:require [clojure.tools.logging :as log]
            [clj-logging-config.log4j :as l4j]
            [swirrl-server.async.jobs :refer [finished-jobs complete-job! restart-id ->Job]]
            [swirrl-server.errors :refer [ex-swirrl encode-error]])
  (:import (java.util UUID)
           (org.apache.log4j MDC)
           (java.util.concurrent PriorityBlockingQueue)
           (java.util.concurrent.locks ReentrantLock)
           (org.openrdf.rio RDFParseException)))

(def priority-levels-map {:sync-write 0 :exclusive-write 1 :batch-write 2})

(def all-priority-types (into #{} (keys priority-levels-map)))

(def compare-jobs (comparator
                   (fn [job1 job2]
                     (let [ordering priority-levels-map
                           {type1 :priority time1 :time} job1
                           {type2 :priority time2 :time} job2]

                       (= -1 (compare [(ordering type1) time1]
                                      [(ordering type2) time2]))))))

(defonce global-writes-lock (ReentrantLock.))

(defonce ^:private writes-queue (PriorityBlockingQueue. 11 compare-jobs))

(defmacro with-lock [& forms]
  `(do
     (log/info "Locking for an :exclusive-write")
     (.lock global-writes-lock)
     (try
       ~@forms
       (finally
         (log/info "Unlocking :exclusive-write lock")
         (.unlock global-writes-lock)))))

;;queue-job :: Job -> ()
(defn queue-job!
  "Adds a write job to the job queue. If the job cannot be queued then
  an ExceptionInfo is thrown with a data map containing a :type key
  mapped to a :job-enqueue-failed value."
  [job]
  (let [req-id (MDC/get "reqId")
        job (if req-id
              (with-meta job {:reqId req-id})
              job)]
    (log/info "Queueing job: " job (meta job))
    (if (= :exclusive-write (:priority job))
      (do
        (log/trace "Queueing :exclusive-write job on queue" writes-queue)
        (.add writes-queue job))
      (if (.tryLock global-writes-lock)
        ;; We try the lock, not because we need the lock, but because we
        ;; need to 503/refuse the addition of an item if the lock is
        ;; taken.
        (try
          (.add writes-queue job)
          (finally
            (.unlock global-writes-lock)))

        (throw (ex-swirrl :writes-temporarily-disabled "Write operations are temporarily unavailable.  Failed to queue job.  Please try again later."))))))

;;await-sync-job! :: Job -> ApiResponse
(defn await-sync-job!
  "Submits a sync job to the queue and blocks waiting for it to
  complete. Returns the result of the job execution."
  [{:keys [value-p priority] :as job}]
  {:pre [(= :sync-write priority)]}
  (queue-job! job)
  @value-p)

(defn- write-loop
  "Start the write loop running.  Note this function does not return
  and is supposed to be run asynchronously on a future or thread.

  Users should normally use start-writer! to set this running."

  []
  (log/info "Writer started waiting for tasks")
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
          (log/info "Executing task" job)

          (if (= :exclusive-write priority)
            (with-lock
              (task-f! job))
            (task-f! job))
          (log/info "Finished task")
          (catch Exception ex
            (log/warn ex "A task raised an error delivering error to promise")
            ;; TODO improve error returned
            (complete-job! job (:body (encode-error ex))
                           ;; {:type :error
                           ;;  :exception ex}
                           ))))
      (log/info "Writer waiting for tasks")
      (recur (.take writes-queue))))

(defn start-writer! []
  (future
    (write-loop)))

(defn stop-writer! [writer]
  (future-cancel writer))
