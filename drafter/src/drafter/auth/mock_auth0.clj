(ns drafter.auth.mock-auth0
  (:require [drafter.auth :as auth]
            [drafter.auth.auth0 :as auth0]
            [integrant.core :as ig]
            [clojure.java.io :as io]))

(defn mock-auth0-auth-method
  "Authentication method which verifies JWT bearer tokens signed with an asymmetric keypair. The usual configuration
   uses drafter.test-common/mock-jwk as the JWK which allows user tokens to be generated with test-common/user-token
   or test-common/token. "
  [auth0-client jwk]
  (reify auth/AuthenticationMethod
    (parse-request [_this request]
      (auth0/parse-request-token auth0-client jwk request))
    (authenticate [_this _request {:keys [:swirrl.auth0/authenticated] :as state}]
      (auth0/authenticate-token state))

    (get-swagger-key [_this] :mock-auth0)

    (get-swagger-description [_this]
      {:heading     "Mock Auth0"
       :description (slurp (io/resource "drafter/auth/mock_auth0_swagger_description.md"))})

    (get-swagger-security-definition [_this]
      {:type "apiKey"
       :name "Authorization"
       :in "header"
       :description "Authentication for JWT bearer tokens signed with mock RSA keypair"})

    (get-operation-swagger-security-requirement [_this _operation]
      [])

    (get-swagger-ui-config [_this] {})))

(derive ::mock-auth0-auth-method ::auth/auth-method)

(defmethod ig/init-key ::mock-auth0-auth-method [_ {:keys [auth0-client jwk] :as _opts}]
  (mock-auth0-auth-method auth0-client jwk))



