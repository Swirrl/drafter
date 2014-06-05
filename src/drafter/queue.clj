(ns drafter.queue
  (:import [java.util.concurrent ArrayBlockingQueue]))

(defn make-queue [capacity]
  (let [fair true]
    (ArrayBlockingQueue. capacity fair)))

(defn offer!
  "Returns true if value was accepted on the queue and false if it
  wasn't."
  [queue msg]
  (.offer queue msg))

(defn peek [queue]
  (-> queue .toArray seq))

(defn take! [queue]
  (.take queue))

(defn clear! [queue]
  (.clear queue))

(defn contains-value? [queue value]
  (.contains queue value))

(defn process-queue [queue f error-fn!]
  (future
     (loop []
       (try
         (let [arguments (take! queue)]
           (apply f arguments))
         (catch java.lang.Exception ex
           (error-fn! ex)))
       (recur))))
