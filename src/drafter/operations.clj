(ns drafter.operations
  (:require [taoensso.timbre :as timbre])
  (:import [java.util.concurrent FutureTask]
           [java.util.concurrent.atomic AtomicBoolean]
           [java.io PipedInputStream PipedOutputStream]))

(def system-clock {:now-fn #(System/currentTimeMillis) :offset-fn +})

(defn now-by [{:keys [now-fn]}]
  (now-fn))

(defn offset [{:keys [offset-fn]} absolute relative]
  (offset-fn absolute relative))

(defn max-event [t1 t2]
  (if (nil? (and t1 t2))
    (or t1 t2)
    (max t1 t2)))

(defn combine-events [existing next]
  {:pre [(some? next)]}
  (max-event existing next))

(defn operation-event [operation-state event]
  (update-in operation-state [:last-event] #(combine-events % event)))

(defn update-latest-operation-event [operations-map operation event]
  (if-let [op-state (get operations-map operation)]
    (update-in operations-map [operation] #(operation-event % event))
    operations-map))

(defn exceeded-total-time? [clock {:keys [operation-timeout started-at]}]
  (let [max-operation-time (offset clock started-at operation-timeout)]
    (> (now-by clock) max-operation-time)))

(defn- exceeded-result-timeout? [clock result-timeout last-result-time]
  (let [now (now-by clock)]
    (> now (offset clock last-result-time result-timeout))))

(defn exceeded-result-time? [clock {:keys [started-at last-event operation-timeout result-timeout]}]
  (exceeded-result-timeout? clock result-timeout (or last-event started-at)))

(defn timed-out? [clock operation-state]
  (or (exceeded-total-time? clock operation-state)
      (exceeded-result-time? clock operation-state)))

(defn get-status-p [timed-out-p operation operation-state]
  (cond (.isDone operation) :completed
        (timed-out-p operation-state) :timed-out
        :else :in-progress))

(defn get-status [clock operation operation-state]
  (get-status-p #(timed-out? clock %) operation operation-state))

(defn- merge-pairs [pairs]
  (apply merge (map #(apply hash-map %) pairs)))

;categorise-f :: Map[k, v] -> ((k, v) -> c) -> Map[c, Map[k, v]]
(defn categorise-f [operations-map categorise-f]
  (let [grouped (group-by #(apply categorise-f %) operations-map)
        merged (map (fn [[k vs]] [k (merge-pairs vs)]) grouped)]
    (into {} merged)))

(defn categorise [clock operations]
  (categorise-f operations (fn [op-ref state] (get-status clock @op-ref state))))

(defn process-categorised [{:keys [in-progress completed timed-out]}]
  (doseq [op (keys timed-out)]
      (future-cancel @op))
  in-progress)

(defn monitor-operations [clock operations]
  (process-categorised (categorise clock operations)))

(defn create-repeating-task-fn [^AtomicBoolean flag delay-ms f]
  (fn []
    (while (.get flag)
      (try
        (do
          (Thread/sleep delay-ms)
          (f))
        (catch Exception ex (timbre/error ex "Error executing task function"))))))

(defn repeating-task [f delay-ms]
  (let [flag (AtomicBoolean. true)
        thread-fn (create-repeating-task-fn flag delay-ms f)
        thread (Thread. thread-fn)]
    (.start thread)
    (fn [] (.set flag false))))

(defn create-monitor
  ([] (create-monitor system-clock clojure.lang.Agent/soloExecutor false 2000))
  ([clock executor owns-executor monitor-period-ms]
     (let [operations (atom {})
           monitor-fn (fn [] (swap! operations #(monitor-operations clock %)))
           stop-monitor-fn (repeating-task monitor-fn monitor-period-ms)]
       {:clock clock :executor executor :operations operations :owns-executor owns-executor :stop-monitor-fn stop-monitor-fn})))

(defn shutdown-monitor [{:keys [executor owns-executor stop-monitor-fn]}]
  (stop-monitor-fn)
  (if owns-executor
    (.shutdown executor)))

(defn create-monitor-publish-fn [key {:keys [clock operations]}]
  (fn []
    (let [event (now-by clock)]
      (swap! operations #(update-latest-operation-event % key event))
      nil)))

(defn create-operation [monitor]
  (let [task-p (promise)
        publish-fn (create-monitor-publish-fn task-p monitor)]
    {:publish publish-fn :operation-ref task-p :monitor monitor}))

(defn connect-operation [{task-p :operation-ref} operation-fn]
  {:pre [(not (realized? task-p))]}
  (let [f (FutureTask. operation-fn)]
    (deliver task-p f)
    f))

(defn- init-operation-state [started-at timeouts]
  (assoc timeouts :started-at started-at))

(defn create-timeouts [result-timeout operation-timeout]
  {:result-timeout result-timeout :operation-timeout operation-timeout})

(defn register-operation [{:keys [monitor operation-ref]} timeouts]
  {:pre [(realized? operation-ref)]}
  (let [{:keys [clock operations]} monitor]
    (letfn [(register [operation-map]
            (if (contains? operation-map operation-ref)
              (throw (IllegalStateException. "Operation already submitted"))
              (assoc operation-map operation-ref (init-operation-state (now-by clock) timeouts))))]
      (swap! operations register)))
  nil)

(defn submit-operation [{:keys [monitor operation-ref] :as operation} timeouts]
  (register-operation operation timeouts)
  (.execute (:executor monitor) @operation-ref))

(defn connect-piped-output-stream
  [func]
  (let [input-stream  (PipedInputStream.)
        output-stream (PipedOutputStream. input-stream)
        f #(with-open [os output-stream]
              (func os))]
    [f input-stream]))
