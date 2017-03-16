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
            [drafter.util :refer [log-time-taken]]
            [swirrl-server.async.jobs :refer [finished-jobs job-failed! restart-id ->Job]]
            [swirrl-server.errors :refer [ex-swirrl encode-error]])
  (:import (org.apache.log4j MDC)
           (java.util.concurrent PriorityBlockingQueue TimeUnit)
           (java.util.concurrent.locks ReentrantLock)
           (java.util.concurrent.atomic AtomicBoolean)))

(def priority-levels-map {:sync-write 0 :exclusive-write 1 :batch-write 2})

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
  [{:keys [priority] :as job}]
  (let [req-id (MDC/get "reqId")
        job (if req-id
              (with-meta job {:reqId req-id})
              job)]
    (log/info "Queueing job: " job (meta job))
    ;;exclusive-writes jobs can always be queued immediately
    ;;other jobs can be queued unless there is an exclusive write job in progress
    (if (or (= :exclusive-write priority)
            (not (.isLocked global-writes-lock)))
      (do
        (log/trace (str "Queueing " priority " job on queue") job)
        (.add writes-queue job)
        nil)
      (throw (ex-swirrl :writes-temporarily-disabled (str "Write operations are temporarily unavailable.  Failed to queue job.  Please try again later."))))))

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
  [flag]
  (log/info "Writer started waiting for tasks")
  (loop []
    (when (.get flag)
      (let [{task-f! :function
             priority :priority
             job-id :id :as job} (.poll writes-queue 200 TimeUnit/MILLISECONDS)]
        (when job
          ;; job exists on the queue so execute it. If the priority is :exclusive-write it needs to be executed
          ;; under the write lock so queue attempts are rejected while the job is running
          (l4j/with-logging-context (assoc
                                      (meta job)
                                      :jobId (str "job-" (.substring (str job-id) 0 8)))
                                    (try
                                      ;; Note that task functions are responsible for the delivery
                                      ;; of the promise and the setting of DONE and also preserve
                                      ;; their job id.

                                      (log-time-taken "task"
                                                      (if (= :exclusive-write priority)
                                                        (with-lock
                                                          (task-f! job))
                                                        (task-f! job)))

                                      (catch Exception ex
                                        (log/warn ex "A task raised an error delivering error to promise")
                                        ;; TODO improve error returned
                                        (job-failed! job ex)))))

        (recur)))))

(defn start-writer! []
  (let [flag (AtomicBoolean. true)
        ^Runnable writer #(write-loop flag)
        t (Thread. writer "Drafter write-loop thread")]
    (.start t)
    {:should-continue flag :thread t}))

(defn stop-writer! [{:keys [should-continue thread]}]
  (.set should-continue false)
  (.join thread))
