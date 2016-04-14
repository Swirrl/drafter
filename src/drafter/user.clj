(ns drafter.user
  (:require [buddy.core.codecs :refer [str->bytes base64->bytes]]
            [buddy.core.bytes :as bytes]
            [drafter.util :as util])
  (:import [org.mindrot.jbcrypt BCrypt]
           [java.net URI]))


(def ^{:doc "Ordered list of roles from least permissions to greatest
permissions."
      } roles [:editor :publisher :manager :system])

(def role->permission-level (zipmap roles (iterate inc 1))) ;; e.g. {:editor 1, :publisher 2, :manager 3}

(defrecord User [email role password-digest])

(def username :email)
(def role :role)
(def password-digest :password-digest)

(defn username->uri
  "Gets a user's URI from their username."
  [username]
  {:pre [(util/validate-email-address username)]}
  (str "mailto:" username))

(def user->uri (comp username->uri username))
(defn uri->username
  "Gets a user's username from their URI."
  [user-uri]
  (let [uri (URI. user-uri)]
    (.getSchemeSpecificPart uri)))

(defn is-known-role? [r]
  (util/seq-contains? roles r))

(defn- get-valid-email [email]
  (if-let [valid (util/validate-email-address email)]
    valid
    (throw (IllegalArgumentException. (str "Not a valid email address: " email)))))

(defn create-user
  "Creates a user with a username, role and password digest which can
  be used to authenticate the user."
  [email role password-digest]
  {:pre [(is-known-role? role)]}
  (let [email (get-valid-email email)]
     (->User email role password-digest)))

(defn create-authenticated-user
  "Create a user without any authentication information which is
  assumed to have previously been authenticated. Once a user has been
  authenticated for a request, their authentication parameters should
  no longer be needed, so users are normalised into a model without
  these parameters."
  [email role]
  {:pre [(is-known-role? role)]}
  (let [email (get-valid-email email)]
    {:email email :role role}))

(defn authenticated!
  "Asserts that the given user has been authenticated and returns a
  representation of the user without authentication information."
  [user]
  (dissoc user :password-digest))

(defn get-summary
  "Returns a map containing summary information about a user."
  [{:keys [email role] :as user}]
  {:username email :role role})

(defn has-role?
  "Takes a user and a role keyword and tests whether the user is
  authorised to operate in that role."
  [{:keys [role] :as user} requested]
  {:pre [(is-known-role? requested)]}
  (<= (role->permission-level requested) (role->permission-level role)))

(defn get-digest [s]
  (BCrypt/hashpw s (BCrypt/gensalt)))

(defn password-valid? [user submitted-key]
  (BCrypt/checkpw submitted-key (password-digest user)))

(defn try-authenticate
  "Tries to authenticate a user with the given candidate
  password. Return a representation of the authenticated user if
  successful, or nil if the authentication failed."
  [user submitted-password]
  (when (password-valid? user submitted-password)
    (authenticated! user)))

(defn has-owner? [draftset]
  (contains? draftset :current-owner))

(defn is-owner? [user {:keys [current-owner] :as draftset}]
  (= (username user) current-owner))

(defn is-submitted-by? [user {:keys [submitted-by] :as draftset}]
  (= (username user) submitted-by))

(defn can-claim? [user {:keys [claim-role claim-user] :as draftset}]
  (or (is-owner? user draftset)
      (and (not (has-owner? draftset))
           (is-submitted-by? user draftset))
      (and (some? claim-role)
           (has-role? user claim-role))
      (= claim-user (username user))))

(defn can-view? [user draftset]
  (or (can-claim? user draftset)
      (is-owner? user draftset)))

(defn permitted-draftset-operations [{:keys [current-owner claim-role] :as draftset} user]
  (cond
   (is-owner? user draftset)
   (util/conj-if
    (has-role? user :publisher)
    #{:delete :edit :submit :claim}
    :publish)

   (can-claim? user draftset)
   #{:claim}

   :else #{}))
