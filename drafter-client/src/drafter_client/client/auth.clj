(ns drafter-client.client.auth
  (:require [buddy.sign.jwt :as jwt]))

(defn- jws-token-for [key user]
  (jwt/sign (merge {:iss "publishmydata" :aud "drafter"} user) key))

(defn jws-auth-header-for [key user]
  (let [token (jws-token-for key user)]
    (str "Token " token)))

(defn jws-auth-header [client user]
  (let [token (jws-token-for (:jws-key client) user)]
    (str "Token " token)))

(defn jws-auth [client user]
  {:name ::auth-header
   :enter (fn [ctx]
            (assoc-in ctx
                      [:request :headers "Authorization"]
                      (jws-auth-header client user)))})
