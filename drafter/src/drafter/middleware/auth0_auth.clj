(ns drafter.middleware.auth0-auth
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [swirrl.auth0.jwt :as jwt]
            [drafter.responses :as response]
            [integrant.core :as ig]
            [clojure.edn :as edn])
  (:import com.auth0.jwk.JwkProviderBuilder
           java.util.concurrent.TimeUnit))

(defn read-role [s]
  (let [[ns role] (some-> s (string/split #":"))]
    (when (= ns "drafter") (keyword role))))

(defn normalize-roles [{:keys [payload] :as token}]
  (let [scopes (->> (some-> payload :scope (string/split #" "))
                    (map read-role)
                    (remove nil?))
        permissions (remove nil? (map read-role (:permissions payload)))]
    (assoc token :roles (set (concat scopes permissions)))))

(defn- find-header [request header]
  (->> (:headers request)
       (filter (fn [[k v]] (re-matches (re-pattern (str "(?i)" header)) k)))
       (first)
       (second)))

(defn- parse-header [request token-name]
  (some->> (find-header request "authorization")
           (re-find (re-pattern (str "^" token-name " (.+)$")))
           (second)))

(defn- access-token-request
  [request access-token jwk iss aud]
  (if access-token
    (let [token (-> jwk
                    (jwt/verify-token iss aud access-token)
                    (normalize-roles))
          status (:status token)]
      (cond-> (assoc request ::authenticated status)
        (= ::jwt/token-verified status)
        (assoc ::access-token (normalize-roles token)
               :identity {:email (-> token :payload :sub)
                          :role (-> token :roles first)})))
    request))

(defmethod ig/init-key :drafter.auth/auth0 [_ opts] opts)

(defmethod ig/init-key :drafter.middleware.auth0-auth/bearer-token
  [_ {{:keys [aud iss]} :auth0 :keys [auth0 jwk] :as opts}]
  (fn [handler]
    (fn [request]
      (let [token (parse-header request "Bearer")]
        (handler (access-token-request request token jwk iss aud))))))

(defmethod ig/init-key :drafter.middleware.auth0-auth/dev-token
  [_ opts]
  (fn [handler]
    (fn [request]
      (handler (merge request opts)))))

(defn unauthorized [msg]
  {:status 401
   :headers {"Content-Type" "text/plain"}
   :body msg})

(defmethod ig/init-key :drafter.middleware.auth0-auth/token-authentication
  [_ {:keys [auth0] :as opts}]
  (fn [handler]
    (fn [{:keys [::authenticated] :as request}]
      (case (::authenticated request)
        ::jwt/token-verified (handler request)
        ::jwt/token-expired (unauthorized "Token expired.")
        ::jwt/claim-invalid (unauthorized "Not authenticated.")
        ::jwt/token-invalid (unauthorized "Not authenticated.")
        (unauthorized "Not authenticated.")))))

(defn authorized? [{:keys [roles] :as token} role]
  (contains? roles role))

(defmethod ig/init-key :drafter.middleware.auth0-auth/authorization [_ _]
  (fn [role]
    (fn [handler]
      (fn [{:keys [::access-token] :as request}]
        (if (authorized? access-token role)
          (handler request)
          (response/forbidden-response "Not authorized."))))))

(s/def ::wrap-auth fn?)
(s/def ::wrap-token fn?)

(defmethod ig/pre-init-spec :drafter.middleware.auth0-auth/wrap-auth [_]
  (s/keys :req-un [::wrap-token ::wrap-auth]))

(defmethod ig/init-key :drafter.middleware.auth0-auth/wrap-auth
  [_ {:keys [wrap-token wrap-auth] :as config}]
  (comp wrap-token wrap-auth))
