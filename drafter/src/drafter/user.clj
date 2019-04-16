(ns drafter.user
  (:require [clojure.spec.alpha :as s]
            [drafter.util :as util]
            [integrant.core :as ig])
  (:import java.net.URI
           org.mindrot.jbcrypt.BCrypt))

(defprotocol UserRepository
  (find-user-by-username [this username]
    "Attempts to find a user with the given user name in the
    underlying store. Returns nil if no such user was found.")
  
  (get-all-users [this]
    "Returns all users in this repository"))

(defrecord User [email role password-digest])

(defmethod ig/init-key :drafter.user/repo [k opts]
  (throw (ex-info "Config error.  Please use a concrete implementation of the user repo instead." {})))

(def ^{:doc "Ordered list of roles from least permissions to greatest
permissions."
      } roles [:editor :publisher :manager :system])

(def role->permission-level (zipmap roles (iterate inc 1))) ;; e.g. {:editor 1, :publisher 2, :manager 3}

(def username :email)
(def role :role)

(defn get-digest
  "Generate the hashed password from the given plaintext password."
  [passwd-str]
  (BCrypt/hashpw passwd-str (BCrypt/gensalt)))

(def password-digest :password-digest)

(defn password-valid? [user submitted-key]
  (BCrypt/checkpw submitted-key (password-digest user)))

(defn username->uri
  "Gets a user's URI from their username."
  [username]
  {:pre [(util/validate-email-address username)]}
  (URI. "mailto" username nil))

(def user->uri (comp username->uri username))
(defn uri->username
  "Gets a user's username from their URI."
  [user-uri]
  (.getSchemeSpecificPart user-uri))

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
  [{:keys [email role] :as user}]
  (create-authenticated-user email role))

(defn try-authenticate
  "Tries to authenticate a user with the given candidate
  password. Return a representation of the authenticated user if
  successful, or nil if the authentication failed."
  [user submitted-password]
  (when (password-valid? user submitted-password)
    (authenticated! user)))

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

(defn- user-token-invalid [token invalid-key info]
  (let [msg (str "User token invalid: " info " (" invalid-key " = '" (invalid-key token) "')")]
    (throw (ex-info msg {:token token}))))

(defn validate-token!
  "Given a user JWT token from a request, returns a record describing the corresponding user.
   Throws a Buddy 'unauthorised' exception if the token is malformed."
  [token]
  (if-let [email (util/validate-email-address (:email token))]
    (let [role (keyword (:role token))]
      (if (is-known-role? role)
        (create-authenticated-user email role)
        (user-token-invalid token :role "Unknown role")))
    (user-token-invalid token :email "Invalid address")))

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

(s/def ::repo (partial satisfies? UserRepository))