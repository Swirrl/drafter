(ns drafter.auth.auth0
  (:require [drafter.auth :as auth]
            [drafter.user :as user]
            [drafter.responses :as response]
            [swirrl.auth0.middleware :as auth0-middleware]
            [integrant.core :as ig]
            [clojure.string :as string]
            [swirrl.auth0.jwt :as jwt]
            [clojure.java.io :as io]))

(defn email [{:keys [payload] :as access-token}]
  (or (get payload (keyword "https://pmd/user/email"))
      (get payload :sub)))

(defn parse-request-token
  "Parses a bearer token from an incoming request if one exists."
  [auth0-client jwk request]
  (let [token-handler (-> identity
                          (auth0-middleware/wrap-bearer-token {:auth0 auth0-client :jwk jwk}))
        token-request (token-handler request)]
    ;; NOTE: :swirrl.auth0/authenticated key is only added if a token was found on the request
    (when (contains? token-request :swirrl.auth0/authenticated)
      (select-keys token-request [:swirrl.auth0/authenticated :swirrl.auth0/access-token]))))

(defn authenticate-token
  "Validates the token parsed on an incoming request by parse-request-token.
   Checks the incoming token is valid and constructs a drafter user."
  [{:keys [:swirrl.auth0/authenticated] :as state}]
  (case authenticated
    ::jwt/token-verified
    (let [access-token (:swirrl.auth0/access-token state)]
      (if-let [email (email access-token)]
        {:email email
         :permissions (->> access-token :payload :permissions
                           (keep #(re-matches #"drafter:(.*)" %))
                           (map (comp keyword second)) set)}
        (auth/authentication-failed)))

    ::jwt/token-expired
    (auth/authentication-failed (response/unauthorized-response "Token expired."))

    ::jwt/claim-invalid (auth/authentication-failed)
    ::jwt/token-invalid (auth/authentication-failed)
    (auth/authentication-failed)))

(defn- update-keys [m f]
  (into {} (map (fn [[k v]] [(f k) v]) m)))

(defn auth0-auth-method
  "Returns an implementation of the AuthenticationMethod protocol which uses auth0. Tokens should be
   specified on incoming requests within an 'Authorization: Bearer <token>' header. The validity of
   the token is checked with the given jwk parameter. The decoded token should contain the user's
   email or it will be rejected."
  [auth0-client jwk]
  (reify auth/AuthenticationMethod
    (parse-request [_this request]
      (parse-request-token auth0-client jwk request))
    (authenticate [_this _request state]
      (authenticate-token state))

    (get-swagger-key [_this] :auth0)

    (get-swagger-description [_this]
      {:heading "Auth0"
       :description (slurp (io/resource "drafter/auth/auth0_swagger_description.md"))})

    (get-swagger-security-definition [_this]
      {:type "oauth2"
       :flow "application"
       :description "OAuth authentication via Auth0"
       :tokenUrl (str (:endpoint auth0-client) "/oauth/token")
       :scopes (update-keys user/permission-summary #(str "drafter" %))})

    (get-operation-swagger-security-requirement [_this {:keys [permission] :as operation}]
      [(str "drafter:" permission)])

    (get-swagger-ui-config [_this] {:auth0Audience (:aud auth0-client)})))

(derive ::auth0-auth-method ::auth/auth-method)

(defmethod ig/init-key ::auth0-auth-method [_ {:keys [auth0-client jwk] :as _opts}]
  (auth0-auth-method auth0-client jwk))
