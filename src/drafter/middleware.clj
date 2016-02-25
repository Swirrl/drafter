(ns drafter.middleware
  (:require [clj-logging-config.log4j :as l4j]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.set :as set]
            [environ.core :refer [env]]
            [selmer.parser :as parser]
            [drafter.responses :as response]
            [drafter.user :as user]
            [drafter.user.repository :as user-repo]
            [buddy.auth :as auth]
            [buddy.auth.backends.httpbasic :refer [http-basic-backend]]
            [buddy.auth.middleware :refer [wrap-authentication wrap-authorization]]
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
  (if-let [user (user-repo/find-user-by-username user-repo username)]
    (if (user/authenticated? user password)
      user)))

(defn basic-authentication [user-repo realm inner-handler]
  (let [conf {:realm realm
              :authfn #(authenticate-user user-repo %1 %2)
              :unauthorized-handler (fn [req err]
                                      (response/unauthorised-basic-response realm))}
        backend (http-basic-backend conf)]
    (wrap-authorization (wrap-authentication inner-handler backend) backend)))

(defn require-authenticated [inner-handler]
  (fn [request]
    (if (auth/authenticated? request)
      (inner-handler request)
      (auth/throw-unauthorized {:message "Authentication required"}))))

(defn require-basic-authentication
  "Wraps a handler in one which requires the request is authenticated
  through HTTP Basic authentication."
  [user-repo realm inner-handler]
  (basic-authentication user-repo realm (require-authenticated inner-handler)))

(defn require-params [required-keys inner-handler]
  (fn [{:keys [params] :as request}]
    (if-let [missing-keys (seq (set/difference required-keys (set (keys params))))]
      (response/unprocessable-entity-response (str "Missing required parameters: " (string/join "," (map name missing-keys))))
      (inner-handler request))))

(defn allowed-methods-handler
  "Wraps a handler with one which checks whether the method of the
  incoming request is allowed with a given predicate. If the request
  method does not pass the predicate a 405 Not Allowed response is
  returned."
  [is-allowed-fn inner-handler]
  (fn [{:keys [request-method] :as request}]
    (if (is-allowed-fn request-method)
      (inner-handler request)
      (response/method-not-allowed-response request-method))))
