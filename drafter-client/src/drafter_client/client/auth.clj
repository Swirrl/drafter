(ns drafter-client.client.auth
  (:require [buddy.sign.jwt :as jwt]))

(defn- jws-token-for [key user]
  (jwt/sign (merge {:iss "publishmydata" :aud "drafter"} user)
            key))

(defn jws-auth-header-for [key user]
  (let [token (jws-token-for key user)]
    (str "Token " token)))
