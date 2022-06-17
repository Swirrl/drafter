(ns drafter.auth.basic
  (:require [drafter.auth :as auth]
            [buddy.auth.backends.httpbasic :as http-basic]
            [drafter.user :as user]
            [clojure.tools.logging :as log]
            [buddy.auth.protocols :as authproto]
            [drafter.responses :as response]
            [integrant.core :as ig]))

(defn- authenticate-user
  "Authenticate the credentials supplied in a request against the user repository.
   Return the user record if successful, otherwise nil."
  [user-repo {:keys [username password] :as auth-data}]
  (log/info "auth user" username password)
  (if-let [user (user/find-user-by-username user-repo username)]
    (user/try-authenticate user password)))

(defn- basic-auth-backend [{:as user-repo :keys [realm]}]
  (let [conf {:realm                (or realm "Drafter")
              :authfn               (fn [_request auth-data]
                                      (authenticate-user user-repo auth-data))
              :unauthorized-handler (fn [req err]
                                      (response/unauthorised-basic-response realm))}]
    (http-basic/http-basic-backend conf)))

(defn basic-auth-method
  "Returns an implementation of the AuthenticationMethod protocol for HTTP basic
   authentication."
  ([user-repo] (basic-auth-method user-repo {}))
  ([user-repo {:keys [realm] :as opts}]
   (let [backend (basic-auth-backend user-repo)
         realm (or realm "Drafter")]
     (reify auth/AuthenticationMethod
       (parse-request [_this request]
         (authproto/-parse backend request))

       (authenticate [_this _request state]
         (if-let [identity (authenticate-user user-repo state)]
           identity
           (auth/authentication-failed (response/unauthorised-basic-response realm))))))))

(derive ::basic-auth-method ::auth/auth-method)

(defmethod ig/init-key ::basic-auth-method [_ {:keys [drafter.user/repo] :as opts}]
  (basic-auth-method repo opts))