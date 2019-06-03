(ns drafter-client.test-util.jwt
  (:require [cheshire.core :as json]
            [clj-time.coerce :refer [to-date]]
            [clj-time.core :as time]
            [integrant.core :as ig])
  (:import com.auth0.jwt.algorithms.Algorithm
           [com.auth0.jwt.exceptions InvalidClaimException
            JWTVerificationException TokenExpiredException]
           [com.auth0.jwk Jwk JwkProvider]
           com.auth0.jwt.JWT
           java.security.KeyPairGenerator
           java.util.Base64))

(defonce keypair
  (-> (KeyPairGenerator/getInstance "RSA")
      (doto (.initialize 4096))
      (.genKeyPair)))

(def pubkey (.getPublic keypair))
(def privkey (.getPrivate keypair))

(def alg (Algorithm/RSA256 pubkey privkey))

(defn token [iss aud sub role]
  (-> (JWT/create)
      (.withIssuer (str iss \/))
      (.withSubject sub)
      (.withAudience (into-array String [aud]))
      (.withExpiresAt (to-date (time/plus (time/now) (time/minutes 10))))
      (.withClaim "scope" role)
      (.sign alg)))

;; (def jwt (token "test" "wut" "auth0-user-id"))

;; (def jwk
;;   (reify JwkProvider
;;     (get [_ _]
;;       (proxy [Jwk] ["" "" "RSA" "" '() "" '() "" {}]
;;         (getPublicKey [] (.getPublic keypair))))))

;; (Base64/getEncoder (.getEncoded pubkey) "UTF-8")

;; (require '[drafter.jwt :refer [verify-token]])

;; (verify-token jwk "test" "wut" jwt)

(defn mock-jwk []
  (reify JwkProvider
    (get [_ _]
      (proxy [Jwk] ["" "" "RSA" "" '() "" '() "" {}]
        (getPublicKey [] (.getPublic keypair))))))
