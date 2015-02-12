(ns drafter.operations
  (:require [taoensso.timbre :as timbre])
  (:import [java.util.concurrent FutureTask]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.io PipedInputStream PipedOutputStream]))

(def system-clock {:now-fn #(System/currentTimeMillis) :offset-fn +})

(defn now-by
  "Gets the current time according to the given clock"
  [{:keys [now-fn]}]
  (now-fn))

(defn offset
  "Gets the time at an offset from an absolute time according to the given clock"
  [{:keys [offset-fn]} absolute relative]
  (offset-fn absolute relative))

(defn max-event
  "Gets the latest of the two given events. A non-nil event is considered later than a nil event."
  [t1 t2]
  (if (nil? (and t1 t2))
    (or t1 t2)
    (max t1 t2)))

(defn combine-events
  "Finds the latest event between the existing and next for an operation. The next event cannot be nil."
  [existing next]
  {:pre [(some? next)]}
  (max-event existing next))

(defn operation-event
  "Updates the latest event for an operation and returns the new state."
  [operation-state event]
  (update-in operation-state [:last-event] #(combine-events % event)))

(defn update-latest-operation-event
  "Updates the latest event for an operation in the operations map. If the operation does not exsist in the map then it is not inserted and the
  event is ignored."
  [operations-map operation event]
  (if-let [op-state (get operations-map operation)]
    (update-in operations-map [operation] #(operation-event % event))
    (do
      (timbre/warn "Received event for unknown operation")
      operations-map)))

(defn exceeded-total-time?
  "Calculates whether the operation has exceeded the total timeout according to the given clock."
  [clock {:keys [operation-timeout started-at]}]
  (let [max-operation-time (offset clock started-at operation-timeout)]
    (> (now-by clock) max-operation-time)))

(defn- exceeded-result-timeout?
  [clock result-timeout last-result-time]
  (let [now (now-by clock)]
    (> now (offset clock last-result-time result-timeout))))

(defn exceeded-result-time?
  "Calculates whether the operation has exceeded the timeout for writing the next result according to the given clock."
  [clock {:keys [started-at last-event operation-timeout result-timeout]}]
  (exceeded-result-timeout? clock result-timeout (or last-event started-at)))

(defn timed-out?
  "Whether an operation has timed out according to a clock."
  [clock operation-state]
  (or (exceeded-total-time? clock operation-state)
      (exceeded-result-time? clock operation-state)))

(defn get-status-p
  "Gets the status of a query operation given its current state and a predicate for calculating whether it has timed out."
  [timed-out-p operation operation-state]
  {:post [(#{:completed, :timed-out, :in-progress} %)]}
  (cond (.isDone operation) :completed
        (timed-out-p operation-state) :timed-out
        :else :in-progress))

(defn get-status
  "Gets the status of an operation with respect to a given clock."
  [clock operation operation-state]
  (get-status-p #(timed-out? clock %) operation operation-state))

;categorise-f :: Map[k, v] -> ((k, v) -> c) -> Map[c, Map[k, v]]
(defn categorise-f
  "Categorises the pairs in a map according to the given categorisation function. The categorisation function should partition
  the pairs in the input map to produce a map of maps where each top-level map contains all pairs in the input with the same
  categorisation."
  [operations-map categorise-f]
  (let [grouped (group-by #(apply categorise-f %) operations-map)
        merged (map (fn [[k vs]] [k (into {} vs)]) grouped)]
    (into {} merged)))

(defn categorise
  "Categorises all operations in the operations map according to their status (completed, timed out, in progress) by the given clock"
  [clock operations]
  (categorise-f operations (fn [op-ref state] (get-status clock @op-ref state))))

(defn cancel-all
  "Given a Map[IDeref[Future], a] cancels all the futures in the keys of the map."
  [operations]
  (doseq [op (keys operations)]
    (future-cancel @op)))

(defn process-categorised
  "Given a categorised map (see categorise) of operations, cancels all those that have timed out and returns the map of those still
  in progress."
  [{:keys [in-progress completed timed-out]}]
  (cancel-all timed-out)
  in-progress)

(defn monitor-operations [clock operations]
  (process-categorised (categorise clock operations)))

(defn create-repeating-task-fn
  "Creates a function which executes a given task function repeatedly while a given flag is set, with a delay between each iteration.
  Any exceptions thrown by the task function are caught and logged and do not stop the iteration."
  [^AtomicBoolean flag delay-ms f]
  (fn []
    (while (.get flag)
      (try
        (do
          (Thread/sleep delay-ms)
          (f))
        (catch Exception ex (timbre/error ex "Error executing task function"))))))

(defn repeating-task
  "Creates a starts a new thread which repeatedly executes the given task function with the specified delay between iterations.
  Returns a no-argument function which can be called to stop the task."
  [f delay-ms]
  (let [flag (AtomicBoolean. true)
        thread-fn (create-repeating-task-fn flag delay-ms f)
        thread (Thread. thread-fn)]
    (.start thread)
    (fn [] (.set flag false))))

(defn create-monitor
  "Creates and starts a monitor with the given clock and ExecutorService. The clock is used to report the time of the last result write
  for each operation and used to calculate time outs. The ExecutorService is used to execute operation tasks - if owns-executor is true
  then shutdown-monitor will shutdown the ExecutorService when called. The monitor period is the number of milliseconds between checks
  for timed-out operations."
  ([] (create-monitor system-clock clojure.lang.Agent/soloExecutor false 2000))
  ([clock executor owns-executor monitor-period-ms]
     (let [operations (atom {})
           monitor-fn (fn [] (swap! operations #(monitor-operations clock %)))
           stop-monitor-fn (repeating-task monitor-fn monitor-period-ms)]
       {:clock clock :executor executor :operations operations :owns-executor owns-executor :stop-monitor-fn stop-monitor-fn})))

(defn shutdown-monitor
  "Shuts down the given monitor. This cancels all running operations and stops the monitoring operation. If the monitor owns its 
  ExecutorService it also shuts it down."
  [{:keys [executor owns-executor stop-monitor-fn operations]}]
  (cancel-all @operations)
  (stop-monitor-fn)
  (if owns-executor
    (.shutdown executor)))

(defn create-monitor-publish-fn
  "Creates a function which when called publishes the last result write time for the operation according to the given clock."
  [key {:keys [clock operations]}]
  (fn []
    (let [event (now-by clock)]
      (swap! operations #(update-latest-operation-event % key event))
      nil)))

(defn create-operation
  "Creates an unconnected operation on the given monitor."
  [monitor]
  (let [task-p (promise)
        publish-fn (create-monitor-publish-fn task-p monitor)]
    {:publish publish-fn :operation-ref task-p :monitor monitor}))

(defn connect-operation
  "Connects an operation to the function to execute when submitted. This must be called before submitting the operation."
  [{task-p :operation-ref} operation-fn]
  {:pre [(not (realized? task-p))]}
  (let [f (FutureTask. operation-fn)]
    (deliver task-p f)
    f))

(defn- init-operation-state [started-at timeouts]
  (assoc timeouts :started-at started-at))

(defn create-timeouts
  "Specifies the timeouts for an operation."
  [result-timeout operation-timeout]
  {:result-timeout result-timeout :operation-timeout operation-timeout})

(defn register-operation
  "Registers an operation in the operations map with the given timeout periods."
  [{:keys [monitor operation-ref]} timeouts]
  {:pre [(realized? operation-ref)]}
  (let [{:keys [clock operations]} monitor]
    (letfn [(register [operation-map]
            (if (contains? operation-map operation-ref)
              (throw (IllegalStateException. "Operation already submitted"))
              (assoc operation-map operation-ref (init-operation-state (now-by clock) timeouts))))]
      (swap! operations register)))
  nil)

(defn submit-operation
  "Submits an operation for execution on its monitor."
  [{:keys [monitor operation-ref] :as operation} timeouts]
  (register-operation operation timeouts)
  (.execute (:executor monitor) @operation-ref))

(defn connect-piped-output-stream
  "Creates a no-argument thunk from a function which takes a single OutputStream argument which it writes to when executed.
  Returns a vector containing the thunk for the write operation and the input stream which will be written to by the write operation."
  [func]
  (let [input-stream  (PipedInputStream.)
        output-stream (PipedOutputStream. input-stream)
        f #(with-open [os output-stream]
              (func os))]
    [f input-stream]))
