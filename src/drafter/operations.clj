(ns drafter.operations
  (:require [taoensso.timbre :as timbre])
  (:import [java.util.concurrent FutureTask TimeUnit Executors]
           [java.io PipedInputStream PipedOutputStream]))

(def system-clock {:now-fn #(System/currentTimeMillis)})

(defn- now-by
  "Gets the current time according to the given clock"
  [{:keys [now-fn]}]
  (now-fn))

(defn- offset
  "Gets the time at an offset from an absolute time according to the
  given clock"
  [clock absolute relative]
  (+ absolute relative))

(defn max-timestamp
  "Gets the latest of the two given times. A non-nil time is
  considered later than a nil time."
  [t1 t2]
  (if (nil? (and t1 t2))
    (or t1 t2)
    (max t1 t2)))

(defn combine-timestamps
  "Finds the latest timestamps between the existing and next for an
  operation. The next timestamp cannot be nil."
  [existing next]
  {:pre [(some? next)]}
  (max-timestamp existing next))

(defn update-timestamp
  "Updates the latest timestamp for an operation and returns the new
  state."
  [operation-state timestamp]
  (update-in operation-state [:timestamp] #(combine-timestamps % timestamp)))

(defn update-operation-timestamp
  "Updates the latest timestamp for an operation in the operations
  map. If the operation does not exsist in the map then it is not
  inserted and the timestamp is ignored."
  [operations-map operation timestamp]
  (if-let [op-state (get operations-map operation)]
    (update-in operations-map [operation] #(update-timestamp % timestamp))
    (do
      (timbre/warn "Received timestamp for unknown operation")
      operations-map)))

(defn exceeded-total-time?
  "Calculates whether the operation has exceeded the total timeout
  according to the given clock."
  [clock {:keys [operation-timeout started-at]}]
  (let [max-operation-time (offset clock started-at operation-timeout)]
    (> (now-by clock) max-operation-time)))

(defn- exceeded-result-timeout?
  [clock result-timeout last-timestamp]
  (let [now (now-by clock)]
    (> now (offset clock last-timestamp result-timeout))))

(defn exceeded-result-time?
  "Calculates whether the operation has exceeded the timeout for
  writing the next result according to the given clock."
  [clock {:keys [started-at timestamp operation-timeout result-timeout]}]
  (exceeded-result-timeout? clock result-timeout (or timestamp started-at)))

(defn timed-out?
  "Whether an operation has timed out according to a clock."
  [clock operation-state]
  (or (exceeded-total-time? clock operation-state)
      (exceeded-result-time? clock operation-state)))

(defn get-status-p
  "Gets the status of a query operation given its current state and a
  predicate for calculating whether it has timed out."
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
  "Categorises the pairs in a map according to the given
  categorisation function. The categorisation function should
  partition the pairs in the input map to produce a map of maps where
  each top-level map contains all pairs in the input with the same
  categorisation."
  [operations-map categorise-f]
  (let [grouped (group-by #(apply categorise-f %) operations-map)
        merged (map (fn [[k vs]] [k (into {} vs)]) grouped)]
    (into {} merged)))

(defn categorise
  "Categorises all operations in the operations map according to their
  status (completed, timed out, in progress) by the given clock"
  [clock operations]
  (categorise-f operations (fn [op-ref state] (get-status clock @op-ref state))))

(defn cancel-all
  "Given a Map[IDeref[Future], a] cancels all the futures in the keys
  of the map."
  [operations]
  (doseq [op (keys operations)]
    (future-cancel @op)))

(defn process-categorised
  "Given a categorised map (see categorise) of operations, cancels all
  those that have timed out and returns the map of those still in
  progress."
  [{:keys [in-progress completed timed-out]}]
  (cancel-all timed-out)
  in-progress)

(defn create-repeating-task-fn
  "Creates a function which tries to execute the given function and
  catches and logs any thrown exceptions."
  [f]
  #(try (f)
     (catch Exception ex (timbre/error ex "Error executing task function"))))

(defn repeating-task
  "Creates a starts a new thread which repeatedly executes the given
  task function with the specified delay between iterations. Returns a
  no-argument function which stops the repeating task when called."
  [f delay-ms]
  (let [executor-service (Executors/newSingleThreadScheduledExecutor)
        task-future (.scheduleWithFixedDelay executor-service (create-repeating-task-fn f) delay-ms delay-ms TimeUnit/MILLISECONDS)]
    (fn []
      (future-cancel task-future)
      (.shutdown executor-service))))

(defn create-reaper-fn
  "Returns a no-argument function which finds inside the operations
  ref all timed-out and completed operations according to the
  clock. Any timed-out operations are cancelled while completed
  operations are removed leaving only the in-progress operations in
  the map pointed to by the ref."
  [operations-ref clock]
  (let [killing-time #(process-categorised (categorise clock %))]
    #(swap! operations-ref killing-time)))

(defn start-reaper
  "Starts a 'reaper' task to periodically find and cancel timed-out
  operations inside the given Atom[Map[IDeref[Future],
  OperationState]]. Returns a no-argument function which cancels the
  reaper task when called."
  ([operations-atom monitor-period-ms] (start-reaper operations-atom monitor-period-ms system-clock))
  ([operations-atom monitor-period-ms clock]
     (let [reaper-fn (create-reaper-fn operations-atom clock)
           stop-reaper-fn (repeating-task reaper-fn monitor-period-ms)]
       stop-reaper-fn)))

(defn create-operation-publish-fn
  "Creates a function which when called publishes the last result
  write time for the operation according to the given clock."
  [key clock operations]
  (fn []
    (swap! operations #(update-operation-timestamp % key (now-by clock)))
    nil))

(defn create-operation
  "Creates an unconnected operation on an operations map with the
  given clock."
  ([operations-atom] (create-operation operations-atom system-clock))
  ([operations-atom clock]
     (let [task-p (promise)
           publish-fn (create-operation-publish-fn task-p clock operations-atom)]
       {:publish publish-fn :operation-ref task-p :clock clock :operations-atom operations-atom})))

(defn connect-operation
  "Connects an operation to the function to execute when
  submitted. This must be called before submitting the operation."
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
  [{:keys [clock operation-ref operations-atom]} timeouts]
  {:pre [(realized? operation-ref)]}
  (letfn [(register [operations-map]
            (if (contains? operations-map operation-ref)
              (throw (IllegalStateException. "Operation already submitted"))
              (assoc operations-map operation-ref (init-operation-state (now-by clock) timeouts))))]
    (swap! operations-atom register)
    nil))

(defn submit-operation
  "Submits an operation for execution on an ExecutorService."
  [{:keys [operation-ref] :as operation} executor-service timeouts]
  (register-operation operation timeouts)
  (.execute executor-service @operation-ref))

(defn connect-piped-output-stream
  "Creates a no-argument thunk from a function which takes a single
  OutputStream argument which it writes to when executed. Returns a
  vector containing the thunk for the write operation and the input
  stream which will be written to by the write operation."
  [func]
  (let [input-stream  (PipedInputStream.)
        output-stream (PipedOutputStream. input-stream)
        f #(with-open [os output-stream]
              (func os))]
    [f input-stream]))
