(ns drafter.user
  (:require [buddy.core.codecs :refer [str->bytes base64->bytes]]
            [buddy.core.bytes :as bytes]
            [drafter.util :as util])
  (:import [org.mindrot.jbcrypt BCrypt]))


(def ^{:doc "Ordered list of roles from least permissions to greatest
permissions."
      } roles [:editor :publisher :manager])

(def role->permission-level (zipmap roles (iterate inc 1))) ;; e.g. {:editor 1, :publisher 2, :manager 3}

(defrecord User [email role api-key-digest])

(def username :email)
(def role :role)
(def api-key :api-key-digest)

(defn create-user [email role api-key-digest]
  {:pre [(util/seq-contains? roles role)]}
  (->User email role api-key-digest))

(defn is-known-role? [r]
  (util/seq-contains? roles r))

(defn has-role?
  "Takes a user and a role keyword and tests whether the user is
  authorised to operate in that role."
  [{:keys [role] :as user} requested]
  {:pre [(is-known-role? requested)]}
  (<= (role->permission-level requested) (role->permission-level role)))

(defn- constant-time-string-equals? [s1 s2]
  (let [b1 (str->bytes s1)
        b2 (str->bytes s2)]
    (bytes/equals? b1 b2)))

(defn get-digest [s]
  (BCrypt/hashpw s (BCrypt/gensalt)))

(defn authenticated? [{:keys [api-key-digest] :as user} submitted-key]
  (BCrypt/checkpw submitted-key api-key-digest))

(defn is-owner? [user {:keys [current-owner] :as draftset}]
  (= (username user) current-owner))

(defn can-claim? [user {:keys [claim-role] :as draftset}]
  (or (is-owner? user draftset)
      (and (some? claim-role)
           (has-role? user claim-role))))

(defn permitted-draftset-operations [{:keys [current-owner claim-role] :as draftset} user]
  (cond
   (is-owner? user draftset)
   (util/conj-if
    (has-role? user :publisher)
    #{:delete :edit :submit}
    :publish)

   (can-claim? user draftset)
   #{:claim}

   :else #{}))
