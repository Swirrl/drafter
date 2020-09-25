(ns drafter-client.auth.auth0
  (:require [clojure.spec.alpha :as s]
            [drafter-client.client.interceptors :as interceptor]
            [drafter-client.client.protocols :as dcpr]
            [integrant.core :as ig]
            [martian.core :as martian]
            [schema.core :as schema]))

(def ^:private auth0-routes
  [{:route-name :oauth-token
    :path "/oauth/token"
    :method :post
    :path-parts ["/oauth/token"]
    :consumes ["application/x-www-form-urlencoded"]
    :produces ["application/json"]
    :summary "Get a jwt auth token from auth0"
    :body-schema {:credentials
                  {:grant_type schema/Str
                   :client_id schema/Str
                   :client_secret schema/Str
                   :audience schema/Str}}}])

(deftype Auth0Client [martian client-id client-secret audience]
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [this k default]
    (case k
      :martian martian
      :client-id client-id
      :client-secret client-secret
      :audience audience
      (.valAt martian k default))))

#_(defn- client [endpoint client-id client-secret audience]
  (let [api (martian/bootstrap endpoint auth0-routes {:interceptors i/default-interceptors})]
    (->Auth0Client api client-id client-secret audience)))

(defn- intercept
  {:style/indent :defn}
  [{:keys [martian client-id client-secret audience]} & interceptors]
  (->Auth0Client (apply update martian :interceptors conj interceptors)
                 client-id
                 client-secret
                 audience))

(defn auth0-interceptor [access-token]
  {:name ::bearer-token
   :enter #(assoc-in % [:request :headers "Authorization"] access-token)})

(defn get-client-id-token
  "Get an auth0 token from the auth0 client suitable for generating an
  Authorization header.

  Returns a map with keys

  :access_token         - the token
  :scope                - e.g. a string drafter:publisher
  :expires_in           - an integer representing how long the token is valid for
  :token_type           - string, type of token e.g. \"Bearer\".

  For using with drafter client you typically just want
  the :access_token from this response.
  "
  [{:keys [auth0] :as client}]
  (let [body {:grant-type "client_credentials"
              :client-id (:client-id auth0)
              :client-secret (:client-secret auth0)
              :audience (:audience auth0)}]
    (-> auth0
        (intercept (interceptor/content-type "application/x-www-form-urlencoded"))
        (intercept (interceptor/set-redirect-strategy :none))
        (martian/response-for :oauth-token body)
        (:body))))


(defrecord Auth0Provider [auth0-client opts]
  dcpr/AuthorizationProvider
  (authorization-header [{:keys [auth0-client] :as t}]
    (str "Bearer " (:access_token (get-client-id-token {:auth0 auth0-client}))))
  (interceptor [t]
    (auth0-interceptor (dcpr/authorization-header t))))

(s/def ::auth0-client some?) ;; todo instance/satisfies check

(defmethod ig/pre-init-spec :drafter-client.auth/auth0-provider [_]
  (s/keys :req-un [::auth0-client]))

(defmethod ig/init-key :drafter-client.auth/auth0-provider [_ opts]
  (map->Auth0Provider opts))
