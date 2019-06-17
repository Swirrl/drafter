(ns drafter.middleware.auth0-auth
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [drafter.responses :as response]
            [integrant.core :as ig]
            [swirrl.auth0.jwt :as jwt]))

(defn authorized? [{:keys [roles] :as token} role]
  (contains? roles role))

(defn unauthorized [msg]
  {:status 401
   :headers {"Content-Type" "text/plain"}
   :body msg})

(defn email [{:keys [payload] :as access-token}]
  (or (get payload (keyword "https://pmd/user/email"))
      (get payload :sub)))

(defmethod ig/init-key :drafter.auth/auth0 [_ opts] opts)

(defmethod ig/init-key :drafter.auth/read-role [_ opts]
  (fn [s]
    (let [[ns role] (some-> s (string/split #":"))]
      (when (and (= ns "drafter") role)
        (keyword role)))))

(defmethod ig/init-key :drafter.middleware.auth0-auth/identify [_ _]
  (fn [handler]
    (fn [{:keys [:swirrl.auth0/access-token] :as request}]
      (let [{:keys [roles]} access-token]
        (-> request
            (assoc :identity {:email (email access-token) :role (first roles)})
            (handler))))))

(defmethod ig/init-key :drafter.middleware.auth0-auth/token-authentication
  [_ {:keys [auth0] :as opts}]
  (fn [handler]
    (fn [{:keys [:swirrl.auth0/authenticated] :as request}]
      (case authenticated
        ::jwt/token-verified (handler request)
        ::jwt/token-expired (unauthorized "Token expired.")
        ::jwt/claim-invalid (unauthorized "Not authenticated.")
        ::jwt/token-invalid (unauthorized "Not authenticated.")
        (unauthorized "Not authenticated.")))))

(defmethod ig/init-key :drafter.middleware.auth0-auth/authorization [_ _]
  (fn [role]
    (fn [handler]
      (fn [{:keys [:swirrl.auth0/access-token] :as request}]
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
