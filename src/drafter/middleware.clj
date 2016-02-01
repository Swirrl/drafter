(ns drafter.middleware
  (:require [clj-logging-config.log4j :as l4j]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [selmer.parser :as parser]
            [swirrl-server.responses :as r]))

(defn log-request [handler]
  (fn [req]
    ;; TODO wrap requests with some kind of ID NDC/MDC
    (l4j/with-logging-context {:reqId (-> (Object.) .hashCode)}
      (log/info "REQUEST " (:uri req) (-> req :headers (get "accept")) (:params req))
      (let [start-time (System/currentTimeMillis)
            resp (handler req)
            total-time (- (System/currentTimeMillis) start-time)]
        (log/info "RESPONSE " (:status resp) "took" (str total-time "ms"))
        resp))))
