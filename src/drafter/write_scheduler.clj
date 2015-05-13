(ns drafter.write-scheduler
  (:require [clojure.tools.logging :as log]
            [swirrl-server.responses :as response]
            [swirrl-server.async.jobs :refer [finished-jobs submitted-job-response complete-job! restart-id ->Job]]
            [drafter.routes.status :refer [finished-job-route]])
  (:import (java.util UUID)
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

(defn create-job [priority f]
  {:pre [(all-priority-types priority)]}
  (->Job (UUID/randomUUID)
         priority
         (System/currentTimeMillis)
         f
         (promise)))

(defn- invalid-rdf-response [job-result]
  (response/api-response 400 {:msg (str "Invalid RDF provided: " job-result)}))

(def ^:private temporarily-locked-for-writes-response
  {:status 503 :body {:type :error :message "Write operations are temporarily unavailable.  Please try again later."}})

(defn blocking-response
  "Block and await the delivery of the jobs result.  When it arrives
  return an appropriate HTTP response."

  [job]
  (let [job-result @(:value-p job)]
    (condp = (class job-result)
      RDFParseException
      (invalid-rdf-response job-result)

      clojure.lang.ExceptionInfo
      (if (= :reading-aborted (-> job-result ex-data :type))
        (invalid-rdf-response job-result)
        (do
          (log/error "Unknown error " job-result)
          (submitted-job-response job-result)))

      Exception
      (submitted-job-response job-result)

      job-result)))

(defn submit-job!
  "Submit a write job to the job queue for execution."

  [job]
  (log/info "Queueing job: " job)
  (if (= :exclusive-write (:priority job))
    (do
      (log/trace "Queueing :exclusive-write job on queue" writes-queue)
      (.add writes-queue job)
      (submitted-job-response job))
    (if (.tryLock global-writes-lock)
      ;; We try the lock, not because we need the lock, but because we
      ;; need to 503/refuse the addition of an item if the lock is
      ;; taken.
      (do (try
            (.add writes-queue job)
            (finally
              (.unlock global-writes-lock)))
          (if (= :sync-write (:priority job))
            (blocking-response job)
            (submitted-job-response job)))

      temporarily-locked-for-writes-response)))

(defn submit-sync-job!
  [job]
  {:pre [(= :sync-write (:priority job))]}
  (submit-job! job))

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
      (try
        ;; Note that task functions are responsible for the delivery
        ;; of the promise and the setting of DONE and also preserve
        ;; their job id.
        (log/info "Executing job" job)

        (if (= :exclusive-write priority)
          (with-lock
            (task-f! job))
          (task-f! job))

        (catch Exception ex
          (log/warn ex "A task raised an error delivering error to promise")
          (complete-job! job {:type :error
                              :exception ex})))

      (log/info "Writer waiting for tasks")
      (recur (.take writes-queue))))

(defn start-writer! []
  (future
    (write-loop)))

(defn stop-writer! [writer]
  (future-cancel writer))
