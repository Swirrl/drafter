(ns drafter.middleware.auth
  (:require [buddy.auth :as auth]
            [buddy.auth.backends.httpbasic :refer [http-basic-backend]]
            [buddy.auth.backends.token :refer [jws-backend]]
            [buddy.auth.middleware :refer [wrap-authentication wrap-authorization]]
            [buddy.auth.protocols :as authproto]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [drafter.logging :refer [with-logging-context]]
            [cognician.dogstatsd :as datadog]
            [drafter.responses :as response]
            [drafter.user :as user]
            [integrant.core :as ig])
  (:import clojure.lang.ExceptionInfo))

(defn- authenticate-user [user-repo request {:keys [username password] :as auth-data}]
  (log/info "auth user" username password)
  (if-let [user (user/find-user-by-username user-repo username)]
    (user/try-authenticate user password)))

(defn- basic-auth-backend [{:as user-repo :keys [realm]}]
  (let [conf {:realm (or realm "Drafter")
              :authfn #(authenticate-user user-repo %1 %2)
              :unauthorized-handler (fn [req err]
                                      (response/unauthorised-basic-response realm))}]
    (http-basic-backend conf)))

(defn- jws-auth-backend [token-auth-key]
  (let [conf {:secret token-auth-key
              :token-name "Token"
              :options {:alg :hs256
                        :iss "publishmydata"
                        :aud "drafter"}}
        inner-backend (jws-backend conf)]
    (reify authproto/IAuthentication
      (-parse [_ request] (authproto/-parse inner-backend request))
      (-authenticate [_ request data]
        (when-let [token (authproto/-authenticate inner-backend request data)]
          (try
            (user/validate-token! token)
            (catch ExceptionInfo ex
              (log/error ex "Token authentication failed due to an invalid user token")
              (auth/throw-unauthorized {:message "Invalid token"}))))))))


(defn require-authenticated
  "Requires the incoming request has been authenticated."
  [inner-handler]
  (fn [request]
    (if (auth/authenticated? request)
      (let [email (:email (:identity request))]
        (with-logging-context {:user email} ;; wrap a logging context over the request so we can trace the user
          (datadog/increment! "drafter.requests.authorised" 1)
          (log/info "got user" email)
          (inner-handler request)))
      (do
        (datadog/increment! "drafter.requests.unauthorised" 1)
        (auth/throw-unauthorized {:message "Authentication required"})))))

(defn- get-configured-token-auth-backend [config]
  (if-let [signing-key (:jws-signing-key config)]
    (jws-auth-backend signing-key)
    (do
      (log/warn "No JWS Token signing key configured - token authentication will not be available")
      (log/warn "To configure JWS Token authentication, specify the jws-signing-key configuration setting")
      nil)))

(defn- get-configured-auth-backends [user-repo config]
  (let [basic-backend (basic-auth-backend user-repo)
        jws-backend (get-configured-token-auth-backend config)]
    (remove nil? [basic-backend jws-backend])))

(defn- wrap-authenticated [auth-backends inner-handler realm]
  (let [auth-handler (apply wrap-authentication (require-authenticated inner-handler) auth-backends)
        unauthorised-fn (fn [req err]
                          (response/unauthorised-basic-response (or realm "Drafter")))]
    (wrap-authorization auth-handler unauthorised-fn)))

(defn make-authenticated-wrapper [{:keys [realm] :as user-repo} config]
  (let [auth-backends (get-configured-auth-backends user-repo config)]
    (fn [inner-handler]
      (wrap-authenticated auth-backends inner-handler realm))))

(defmethod ig/pre-init-spec :drafter.middleware.auth/wrap-auth [_]
  (s/keys :req [::user/repo]))

(defmethod ig/init-key :drafter.middleware.auth/wrap-auth [_ {:keys [::user/repo] :as config}]
  (make-authenticated-wrapper repo config))
