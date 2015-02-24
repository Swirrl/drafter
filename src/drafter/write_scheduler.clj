(ns drafter.write-scheduler
  (:require [clojure.tools.logging :as log]
            [drafter.common.api-routes :as api-routes])
  (:import (java.util UUID)
           (java.util.concurrent PriorityBlockingQueue)
           (java.util.concurrent.locks ReentrantLock)
           (org.openrdf.rio RDFParseException)))

(def ^:private global-writes-lock (ReentrantLock.))

(def compare-jobs (comparator
                   (fn [job1 job2]
                     (let [ordering {:sync 0 :make-live 1 :batch 2}
                           {type1 :type time1 :time} job1
                           {type2 :type time2 :time} job2]

                       (= -1 (compare [(ordering type1) time1]
                                      [(ordering type2) time2]))))))

(def ^:private writes-queue (PriorityBlockingQueue. 11 compare-jobs))

;; TODO consider a done map

(defmacro with-lock [& forms]
  `(do
     (.lock global-writes-lock)
     (try
       ~@forms
       (finally
         (.unlock global-writes-lock)))))

(defrecord Job [id type time function value-p])

(defn create-job [type f & metadata]
  {:pre [(#{:sync :make-live :batch} type)]}
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

(defn submit-job! [job & [metadata]]
  (let [job (with-meta job metadata)]
    (if (= :make-live (:type job))
      (do (.add writes-queue (assoc job
                                    :function (fn []
                                                (with-lock
                                                  ((:function job))))))
          (submitted-job-response job))
      (if (.tryLock global-writes-lock)
        ;; We try the lock, not because we need the lock, but because we
        ;; need to 503/refuse the addition of an item if the lock is
        ;; taken.
        (do (try
              (.add writes-queue job)
              (finally
                (.unlock global-writes-lock)))
            (if (= :sync (:type job))
              (blocking-response job)
              (submitted-job-response job)))
        {:status 503}))))

(def finished-jobs (atom {}))

(defn start-writer! []
  (future
    (log/info "Writer started waiting for tasks")
    (loop [{task-f :function
            type :type
            job-id :id
            promis :value-p :as job} (.take writes-queue)]
      (try
        ;; leave logging of task up to the task
        (let [res (task-f)]
          (deliver promis res))
        (catch Exception ex
          (log/warn "A task raised an error delivering error to promise" ex)
          (deliver promis {:type :error
                           :exception ex})))
      (swap! finished-jobs assoc job-id promis)
      (log/info "Writer waiting for tasks")
      (recur (.take writes-queue)))))

(defn stop-writer! [writer]
  (future-cancel writer))
