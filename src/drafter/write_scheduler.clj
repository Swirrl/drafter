(ns drafter.write-scheduler
  (:require [clojure.tools.logging :as log]
            [drafter.common.api-routes :as api-routes])
  (:import (java.util UUID)
           (java.util.concurrent PriorityBlockingQueue)
           (java.util.concurrent.locks ReentrantLock)
           (org.openrdf.rio RDFParseException)))

(def priority-levels-map {:sync-write 0 :exclusive-write 1 :batch-write 2})

(def all-priority-types (into #{} (keys priority-levels-map)))

(def compare-jobs (comparator
                   (fn [job1 job2]
                     (let [ordering priority-levels-map
                           {type1 :type time1 :time} job1
                           {type2 :type time2 :time} job2]

                       (= -1 (compare [(ordering type1) time1]
                                      [(ordering type2) time2]))))))

(defonce ^:private global-writes-lock (ReentrantLock.))

(def ^:private writes-queue (PriorityBlockingQueue. 11 compare-jobs))

(defonce ^{:doc "Map of finished jobs to promises containing their results."}
  finished-jobs (atom {}))

(defmacro with-lock [& forms]
  `(do
     (log/info "Locking for an :exclusive-write")
     (.lock global-writes-lock)
     (try
       ~@forms
       (finally
         (log/info "Unlocking :exclusive-write lock")
         (.unlock global-writes-lock)))))

(defrecord Job [id type time function value-p])

(defn create-job [type f & metadata]
  {:pre [(all-priority-types type)]}
  (->Job (UUID/randomUUID)
         type
         (System/currentTimeMillis)
         f
         (promise)))

(defn invalid-rdf-response [job-result]
  (api-routes/api-response 400 {:msg (str "Invalid RDF provided: " job-result)}))

(defn submitted-job-response [job]
  {:status 202 :body {:type :ok :id (:id job)}})

(defn unknown-error-response [job-result]
  (api-routes/api-response 500 {:msg (str "Unknown error: " job-result)}))

(def temporarily-locked-for-writes-response
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
  [job & [metadata]]
  (let [job (with-meta job metadata)]
    (log/info "Queueing job: " job)
    (if (= :exclusive-write (:type job))
      (do
        (log/trace "Queueing :exclusive-write job")
        (.add writes-queue (assoc job
                                  :function (fn [job writes-queue]
                                              (with-lock
                                                (let [fun (:function job)]
                                                  (fun job writes-queue))))))
          (submitted-job-response job))
      (if (.tryLock global-writes-lock)
        ;; We try the lock, not because we need the lock, but because we
        ;; need to 503/refuse the addition of an item if the lock is
        ;; taken.
        (do (try
              (.add writes-queue job)
              (finally
                (.unlock global-writes-lock)))
            (if (= :sync-write (:type job))
              (blocking-response job)
              (submitted-job-response job)))

        temporarily-locked-for-writes-response))))

(defn complete-job!
  "Adds the job to the state map of finished-jobs and delivers the
  supplied result to the jobs promise, which will cause blocking jobs
  to unblock, and give job consumers the ability to receive the
  value."
  [job result]
  (let [{job-id :id promis :value-p} job]
    (deliver promis result)
    (swap! finished-jobs assoc job-id promis)
    (log/info "Job " job-id "complete")))

(defn start-writer! []
  (future
    (log/info "Writer started waiting for tasks")
    (loop [{task-f! :function
            type :type
            job-id :id
            promis :value-p :as job} (.take writes-queue)]
      (try
        ;; Task functions are responsible for the delivery of the
        ;; promise and the setting of DONE and also preserve their job
        ;; id.
        (task-f! job writes-queue)
        (catch Exception ex
          (log/warn ex "A task raised an error delivering error to promise")
          (complete-job! job {:type :error
                              :exception ex})))

      (log/info "Writer waiting for tasks")
      (recur (.take writes-queue)))))

(defn stop-writer! [writer]
  (future-cancel writer))
