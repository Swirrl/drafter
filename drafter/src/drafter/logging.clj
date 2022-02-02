(ns drafter.logging
  (:require [clojure.tools.logging :as log])
  (:import org.apache.logging.log4j.ThreadContext
           java.util.UUID))

(defn capture-logging-context []
  (into {} (ThreadContext/getContext)))

;; Modified from withdrawn library:
;; https://github.com/malcolmsparks/clj-logging-config/blob/master/src/main/clojure/clj_logging_config/log4j.clj
(defmacro with-logging-context [x & body]
  `(let [x# ~x
         ctx# (capture-logging-context)]
     (try
       (if (map? x#)
         (run! (fn [[k# v#]]
                 (when-not (nil? v#)
                   (ThreadContext/put (name k#) (str v#)))) x#)
         (ThreadContext/push (str x#)))
       ~@body
       (finally
        (if (map? x#)
          (run! (fn [[k# v#]]
                  (ThreadContext/remove (name k#))
                  (when-let [old# (get ctx# (name k#))]
                    (ThreadContext/put (name k#) (str old#)))) x#)
          (ThreadContext/pop))))))

(defn log-request
  "A ring middleware that logs HTTP requests and responses in the
  Swirrl house-style.  It runs each request in a log4j MDC scope and
  sets a request id to every request.  Additionally it logs the
  time spent serving each request/response.
  Takes an optional map of parameters to scrub from logs, e.g. a map
  like this:
  {:param-a \"scrubbed\"
   :param-b \"hidden\"}
  Will ensure that :param-a is logged as \"scrubbed\" by this
  middleware and that the value at :param-b is replaced by the string
  \"hidden\"."
  ([handler]
   (log-request handler {}))
  ([handler scrub-map]
   (let [scrub (fn [acc [k v]]
                 (if (acc k)
                   (assoc acc k v)
                   acc))]
     (fn [req]
       (let [start-time (System/currentTimeMillis)]
         (with-logging-context {:reqId (str "req-" (-> (UUID/randomUUID) str (.substring 0 8)))
                                :start-time start-time}
           (let [logable-params (reduce scrub
                                        (:params req {})
                                        scrub-map)]

             (log/info "REQUEST" (:uri req) (-> req :headers (get "accept")) logable-params))
           (let [resp (handler req)
                 headers-time (- (System/currentTimeMillis) start-time)]
             (if (instance? java.io.InputStream (:body resp))
               (log/info "RESPONSE" (:status resp) "headers sent after" (str headers-time "ms") "streaming body...")
               (log/info "RESPONSE " (:status resp) "finished.  It took" (str headers-time "ms") "to execute"))

             resp)))))))
