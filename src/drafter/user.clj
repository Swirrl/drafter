(ns drafter.user
  (:require [buddy.core.codecs :refer [str->bytes]]
            [buddy.core.bytes :as bytes]
            [drafter.util :as util]))

(def roles [:editor :publisher :manager])
(defrecord User [email role api-key-digest])
(def username :email)
(def role :role)

(defn create-user [email role api-key-digest]
  {:pre [(util/seq-contains? roles role)]}
  (->User email role api-key-digest))

(defn is-known-role? [r]
  (util/seq-contains? roles r))

(defn has-role? [{:keys [role] :as user} requested]
  {:pre [(is-known-role? requested)]}
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

(defn is-owner? [user {:keys [current-owner] :as draftset}]
  (= (username user) current-owner))

(defn can-claim? [user {:keys [claim-role] :as draftset}]
  (or (is-owner? user draftset)
      (and (some? claim-role)
           (has-role? user claim-role))))

(defn permitted-draftset-operations [{:keys [current-owner claim-role] :as draftset} user]
  (let [role (role user)
        username (username user)]
    (cond
     (is-owner? user draftset)
     (util/conj-if
        (has-role? user :publisher)
        #{:delete :edit :offer}
        :publish)

     (can-claim? user draftset)
     #{:claim}

     :else #{})))
