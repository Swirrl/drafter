(ns drafter-client.auth.basic-auth
  (:require [drafter-client.client.protocols :as dcpr]
            [integrant.core :as ig]
            [clojure.spec.alpha :as s]))

(defn basic-auth-header
  "Given a username and a password return the encoded value for Basic
  Auth as a string. This value can be included as the value for an
  HTTP Authorization header."
  [user password]
  (let [bpass (.getBytes (str user \: password))]
    (str "Basic "
         (-> (java.util.Base64/getEncoder)
             (.encodeToString bpass)))))

(defn basic-auth-interceptor
  "WARNING: end users of this library probably want to include a
  BasicAuthProvider inside drafter-client, rather than calling this
  function directly.

  Returns an interceptor implementing basic auth suitable for use with
  martian."
  [{:keys [user password]}]
  {:name ::auth-header
   :enter (fn [ctx]
            (assoc-in ctx [:request :headers "Authorization"]
                      (basic-auth-header user password)))})

(defrecord BasicAuthProvider [user password]
  dcpr/AuthorizationProvider
  (authorization-header [t]
    (basic-auth-header user password))
  (interceptor [t]
    (basic-auth-interceptor t)))

(s/def ::user string?)
(s/def ::password string?)

(defmethod ig/pre-init-spec :drafter-client.client.auth/basic-auth [_]
  (s/keys :req-un [::user ::password]))

(defmethod ig/init-key :drafter-client.client.auth/basic-auth [_ opts]
  (map->BasicAuthProvider opts))
