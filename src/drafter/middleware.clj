(ns drafter.middleware
  (:require [clojure.tools.logging :as log]
            [clj-logging-config.log4j :as l4j]
            [selmer.parser :as parser]
            [environ.core :refer [env]]))

(defn log-request [handler]
  (fn [req]
    ;; TODO wrap requests with some kind of ID NDC/MDC
    (l4j/with-logging-context {:reqId (-> (Object.) .hashCode)}
      (log/info "REQUEST " (:uri req) (-> req :headers (get "accept")))
      (let [start-time (System/currentTimeMillis)
            resp (handler req)
            total-time (- (System/currentTimeMillis) start-time)]
        (log/info "RESPONSE " (:status resp) "took" (str total-time "ms"))
        resp))))

(defn template-error-page [handler]
  (if (env :dev)
    (fn [request]
      (try
        (handler request)
        (catch clojure.lang.ExceptionInfo ex
          (let [{:keys [type error-template] :as data} (ex-data ex)]
            (if (= :selmer-validation-error type)
              {:status 500
               :body (parser/render error-template data)}
              (throw ex))))))
    handler))
