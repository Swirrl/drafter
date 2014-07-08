(ns drafter.rdf.queue
  (:import [java.util.concurrent ArrayBlockingQueue]))

(defn make-queue [capacity]
  (let [fair true]
    (ArrayBlockingQueue. capacity fair)))

(defn offer!
  "Returns true if value was accepted on the queue and false if it
  wasn't."
  [queue msg]
  (let [uuid (java.util.UUID/randomUUID)
        job (with-meta {:id uuid} msg)]
    (if (.offer queue job)
      uuid
      false)))

(defn peek-jobs [queue]
  (map (fn [m]
         (merge m (meta m)))
       (-> queue .toArray seq)))

(defn take! [queue]
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

(defn process-queue [queue f error-fn!]
  (future
    (try
      (loop []
        (let [arguments (take! queue)]
          (f arguments)
          (recur)))
      (catch java.lang.Exception ex
        (error-fn! ex)))))
