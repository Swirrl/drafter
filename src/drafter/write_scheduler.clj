(ns drafter.write-scheduler
  (:import [java.util.concurrent.locks ReentrantLock]
           [java.util.concurrent PriorityBlockingQueue]
           [java.util UUID]))

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

(defn create-job [type f]
  {:pre [(#{:sync :make-live :batch} type)]}
  (->Job (UUID/randomUUID)
         type
         (System/currentTimeMillis)
         f
         (promise)))

(defn submit-job! [job]
  (if (= :make-live (:type job))
    (.add writes-queue (assoc job
                              :function (fn []
                                          (with-lock
                                            ((:function job))))))
    (if (.tryLock global-writes-lock)
      (do (try
            (.add writes-queue job)
            (finally
              (.unlock global-writes-lock)))
          (if (= :sync (:type job))
            {:status 201 :body @(:value-p job)}
            {:status 202 :body (:id job)}))
      {:status 503})))

(defn start-writer []
  (future
    (loop [{task-f :function
            type :type
            promis :value-p :as job} (.take writes-queue)]
      (try
        (let [res (task-f)]
          (println res)
          (deliver promis res))
        (catch Exception ex
          (println "errored" ex)
          (deliver promis ex)))
      (recur (.take writes-queue)))))

(def writer (start-writer))
