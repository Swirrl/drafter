(ns drafter-client.auth.auth0.m2m
  "A drafter-client auth-provider that supports the auth0 m2m flow.

  This is suited to ETL flows etc, where a client wants to acquire a
  token for a session and use it on all subsequent requests to
  drafter.

  Currently the tokens are not refreshed, but should be valid for
  24hrs (see issue: https://github.com/Swirrl/drafter/issues/454)"
  (:require [clojure.spec.alpha :as s]
            [drafter-client.client.interceptors :as interceptor]
            [drafter-client.client.protocols :as dcpr]
            [integrant.core :as ig]
            [martian.core :as martian]))

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

(defn- valid-token? [{:keys [state-atom]}]
  ;; if token is nil return false
  ;;
  ;; In the future we may extend this predicate to also test for the
  ;; freshness of the token.
  (some? @state-atom))

(defn- cache-token
  "Takes an auth-provider with a :state-atom and an :auth0-client and a
  function fetch-fn that returns a token.

  Stores the token in the state-atom, and returns the cached token on
  subsequent calls."

  ;; NOTE we could in principle have just memoized f here, but I
  ;; figured it was better to write it out ourselves so we can more
  ;; easily extend the implementation to support refreshing the token
  ;; or fetching a new one on an authentication error etc.
  [{:keys [auth0-client state-atom] :as auth-provider} fetch-fn]
  (if (valid-token? auth-provider)
    @state-atom
    (let [token (fetch-fn {:auth0 auth0-client})]
      (reset! state-atom token))))

(defrecord M2MProvider [auth0-client state-atom fetch-fn opts]
  dcpr/AuthorizationProvider
  (authorization-header [{:keys [auth0-client] :as t}]
    (str "Bearer " (:access_token (cache-token (assoc t :auth0 auth0-client)
                                               fetch-fn))))
  (interceptor [t]
    (auth0-interceptor (dcpr/authorization-header t))))

(defn build-auth-provider
  "Construct a new auth0 m2m auth-provider, use this in preference to
  the ->Auth0Provider constructors.  Expects a map with the keys:

    :auth0-client (required)

    :fetch-fn (optionally) a side effecting function that can fetch an
               auth0 access-token. Defaults to get-client-id-token.

    :state-atom (defaults to (atom nil))"
  [opts]
  (-> (merge {:fetch-fn get-client-id-token
              :state-atom (atom nil)}
             opts)
      map->M2MProvider))

(s/def ::auth0-client some?) ;; todo instance/satisfies check

(defmethod ig/pre-init-spec :drafter-client.auth.auth0/m2m-provider [_]
  (s/keys :req-un [::auth0-client]))

(defmethod ig/init-key :drafter-client.auth.auth0/m2m-provider [_ opts]
  (build-auth-provider opts))
