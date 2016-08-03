(ns drafter.middleware
  (:require [clj-logging-config.log4j :as l4j]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [selmer.parser :as parser]
            [swirrl-server.responses :as r])
  (:import [java.util UUID]))

(defn log-request [handler]
  (fn [req]
    ;; TODO wrap requests with some kind of ID NDC/MDC
    (let [start-time (System/currentTimeMillis)]
      (l4j/with-logging-context {:reqId (str "req-" (-> (UUID/randomUUID) str (.substring 0 8)))
                                 :start-time start-time}
        (log/info "REQUEST" (:uri req) (-> req :headers (get "accept")) (:params req))
        (let [resp (handler req)
              headers-time (- (System/currentTimeMillis) start-time)]
          (if (instance? java.io.InputStream (:body resp))
            (log/info "RESPONSE" (:status resp) "headers sent after" (str headers-time "ms") "streaming body...")
            (log/info "RESPONSE " (:status resp) "finished.  It took" (str headers-time "ms") "to execute"))

          resp)))))
