(ns drafter.rdf.queue
  (:import [java.util.concurrent ArrayBlockingQueue]
           [java.util UUID]))

(defn make-queue
  "Make a blocking queue object with a specified capacity."
  [capacity]
  (let [fair true]
    (ArrayBlockingQueue. capacity fair)))

(defn offer!
  "Returns true if value was accepted on the queue and false if it
  wasn't."
  [queue msg]
  (let [uuid (UUID/randomUUID)
        job (with-meta {:id uuid} msg)]
    (if (.offer queue job)
      uuid
      false)))

(defn peek-jobs
  "Peek at all the jobs on the queue without removing any."
  [queue]
  (map (fn [m]
         (merge m (meta m)))
       (-> queue .toArray seq)))

(defn take!

  "Take a job from the queue."
  [queue]
  (let [v (.take queue)]
    (if (map? v)
      (meta v)
      false)))

(defn clear! [queue]
  (.clear queue))

(defn remove-job [queue id]
  (let [job {:id id}]
    (.remove queue job)))

(defn contains-value? [queue id]
  (let [job {:id id}]
    (.contains queue job)))

(defn size
  "Returns the size of the queue."
  [queue]
  (.size queue))

(defn ->uuid [job-id]
  (if (instance? UUID job-id)
    job-id
    (UUID/fromString job-id)))

(defn find-job [queue job-id]
  (let [job-id (->uuid job-id)]
    (->> (peek-jobs queue)
         (filter (fn [i] (= job-id (:id i))))
         first)))

(defn process-queue [queue f error-fn!]
  (future
    (try
      (loop []
        (let [arguments (take! queue)]
          (f arguments)
          (recur)))
      (catch java.lang.Exception ex
        (error-fn! ex)))))
