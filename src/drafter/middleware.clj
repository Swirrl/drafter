(ns drafter.middleware
  (:require [clj-logging-config.log4j :as l4j]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [selmer.parser :as parser]
            [drafter.user :as user]
            [drafter.user.repository :as user-repo]
            [buddy.auth.backends.httpbasic :refer [http-basic-backend]]
            [buddy.auth.middleware :refer [wrap-authentication]]
            [cemerick.friend :as friend]
            [cemerick.friend.workflows :as friend-workflows]))

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

(defn template-error-page [handler]
  (if (env :dev)
    (fn [request]
      (try
        (handler request)
        (catch clojure.lang.ExceptionInfo ex
          (log/warn ex)
          (let [{:keys [type error-template] :as data} (ex-data ex)]
            (if (= :selmer-validation-error type)
              {:status 500
               :body (parser/render error-template data)}
              (throw ex))))))
    handler))

(defn- authenticate-user [user-repo request {:keys [username password] :as auth-data}]
  (if-let [user (user-repo/find-user-by-email-address user-repo username)]
    (if (user/authenticated? user password)
      user)))

(defn basic-authentication [user-repo realm inner-handler]
  (let [backend (http-basic-backend {:realm realm :authfn #(authenticate-user user-repo %1 %2)})]
    (wrap-authentication inner-handler backend)))
