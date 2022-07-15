(ns drafter.auth.jws
  (:require [drafter.auth :as auth]
            [buddy.auth.backends.token :as auth-token]
            [buddy.auth.protocols :as authproto]
            [integrant.core :as ig]
            [drafter.user :as user]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io])
  (:import clojure.lang.ExceptionInfo))

(defn- get-jws-auth-backend [token-auth-key]
  (let [conf {:secret token-auth-key
              :token-name "Token"
              :options {:alg :hs256
                        :iss "publishmydata"
                        :aud "drafter"}
              :on-error (fn [_req ex] (throw ex))}]
    (auth-token/jws-backend conf)))

(defn jws-auth-method
  "Creates an authentication method which looks for signed JWS tokens on incoming
   requests. The token should be specified in the Authorization header as 'Token <jws-token>'.
   The token should contain a user document containing the user email and role, along with an
   issuer of 'publishmydata' and audience of 'drafter'. The token must be signed with the given
   signing key using hmac-sha-256."
  [jws-signing-key]
  (when (nil? jws-signing-key)
    (throw (ex-info "JWS signing key cannot be nil" {})))

  (let [backend (get-jws-auth-backend jws-signing-key)]
    (reify auth/AuthenticationMethod
      (parse-request [_this request]
        (authproto/-parse backend request))

      (authenticate [_this request state]
        (if-let [token (authproto/-authenticate backend request state)]
          (try
            (user/validate-token! token)
            (catch ExceptionInfo ex
              (log/error ex "Token authentication failed due to an invalid user token")
              (auth/authentication-failed)))
          (auth/authentication-failed)))

      (get-swagger-key [_this] :jws-auth)

      (get-swagger-description [_this]
        {:heading "JWS Authentication"
         :description (slurp (io/resource "drafter/auth/jws_swagger_description.md"))})

      (get-swagger-security-definition [_this]
        {:type "apiKey"
         :name "Authorization"
         :in "header"
         :description "Token authentication with a signed JWS token"})

      (get-operation-swagger-security-requirement [_this _operation] [])

      (get-swagger-ui-config [_this] {}))))

(derive ::jws-auth-method ::auth/auth-method)

(defmethod ig/init-key ::jws-auth-method [_ {:keys [jws-signing-key]}]
  (jws-auth-method jws-signing-key))
