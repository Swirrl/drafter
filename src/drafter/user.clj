(ns drafter.user
  (:require [buddy.core.codecs :refer [str->bytes]]
            [buddy.core.bytes :as bytes]))

(def roles #{:editor :publisher :manager})
(defrecord User [email role api-key-digest])
(def username :email)

(defn create-user [email role api-key-digest]
  {:pre [(contains? roles role)]}
  (->User email role api-key-digest))

(defn has-role? [{:keys [role] :as user} requested]
  {:pre [(contains? roles requested)]}
  (case role
    :manager true
    :publisher (not= requested :manager)
    :editor (= requested :editor)))

(defn- constant-time-string-equals? [s1 s2]
  (let [b1 (str->bytes s1)
        b2 (str->bytes s2)]
    (bytes/equals? b1 b2)))

(defn authenticated? [{:keys [api-key-digest] :as user} key]
  (constant-time-string-equals? api-key-digest key))
