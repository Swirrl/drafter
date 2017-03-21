(ns drafter.channels
  (:import [java.util.concurrent ArrayBlockingQueue]))

(defn create-send-once-channel
  "Creates a channel which allows a single value to be sent from the sender to the receiver.
   Returns a pair of functions [send recv] where send is a multi-arity function which allows
   either an ok or error condition to be communicated to the receiver. Senders signal ok by calling
   the function with no arguments and signal an error by calling it with a single value derived
   from java.lang.Throwable. The receive function takes a period and a java.util.concurrent.TimeUnit
   specifying the units. The returned result indicates whether the receiver signalled ok, signalled
   an error or failed to signal anything before the receiver period expired. Which of these outcomes
   occured can be obtained using the channel-ok?, channel-error? and channel-timeout? functions.

   (let [[send recv] (create-send-once-channel)]
     (send)                      ;;indicate success
     (send (RuntimeException.))  ;;indicate error

     ;; in receiver thread
     (let [result (recv 1 TimeUnit/SECONDS)]
       (cond (channel-ok? result)      ;;handle ok
             (channel-error? result)   ;;handle error
             (channel-timeout? result) ;;handle timeout
             )))
     "
  []
  (let [queue (ArrayBlockingQueue. 1)
        sent (atom false)
        send-any (fn [v]
                   (when-not @sent
                     (.add queue v)
                     (reset! sent true)))
        send (fn
               ([] (send-any :ok))
               ([ex]
                (if (instance? Throwable ex)
                  (send-any ex)
                  (throw (IllegalArgumentException. "Throwable required on failure")))))
        recv (fn [timeout time-unit]
               (.poll queue timeout time-unit))]
    [send recv]))

(defn channel-ok? [v] (= :ok v))
(defn channel-error? [v] (instance? Throwable v))
(defn channel-timeout? [v] (nil? v))

