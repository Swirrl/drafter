(ns drafter.user
  (:require [drafter.util :as util]
            [integrant.core :as ig])
  (:import java.net.URI
           org.mindrot.jbcrypt.BCrypt))

(defprotocol UserRepository
  (find-user-by-username [this username]
    "Attempts to find a user with the given user name in the
    underlying store. Returns nil if no such user was found.")
  
  (get-all-users [this]
    "Returns all users in this repository"))

(defrecord User [email permissions password-digest])

(defmethod ig/init-key :drafter.user/repo [k opts]
  (throw (ex-info "Config error.  Please use a concrete implementation of the user repo instead." {})))

(defn role->permissions
  "This function is only intended to be used for compatibility with mongo user
   storage, or for convenience in tests."
  [role]
  (case role
    :norole #{}
    :access #{:public:view}
    :editor (conj (role->permissions :access)
                  :draft:claim :draft:create :draft:delete :draft:edit
                  :draft:submit :draft:share :draft:view :job:view :user:view)
    :publisher (conj (role->permissions :editor) :draft:publish)
    ;; :manager is used in tests to demonstrate scoped claim permissions.
    :manager (conj (role->permissions :publisher) :draft:claim:manager)
    :system (recur :manager)))

(defn roles-including
  "Returns the set of roles which include the given role"
  [role]
  (let [required-level (role->permission-level role)]
    (->> role->permission-level
         (keep (fn [[candidate level]]
                 (if (>= level required-level)
                   candidate)))
         (set))))

(defn get-role-summary
  "Returns a brief summary of the given role"
  [role]
  ({:access "Read-only access"
    :editor "Create and edit access to drafts"
    :publisher "Create, edit and publish access to drafts"
    :manager "Full access to drafts"
    :system "Full access to the entire system"} role))

(def roles #{:access :editor :publisher :manager :system})

(def username :email)

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
  (URI. "mailto" username nil))

(def user->uri (comp username->uri username))
(defn uri->username
  "Gets a user's username from their URI."
  [user-uri]
  (.getSchemeSpecificPart user-uri))

(defn is-known-role? [r]
  (contains? roles r))

(defn- get-valid-email [email]
  (if-let [valid (util/validate-email-address email)]
    valid
    (throw (IllegalArgumentException. (str "Not a valid email address: " email)))))

(defn create-user
  "Creates a user with a username, role and password digest which can be used
   to authenticate the user. Grants the user permissions corresponding to the
   given role."
  [email role password-digest]
  (let [email (get-valid-email email)]
     (->User email (role->permissions role) password-digest)))

(defn create-authenticated-user
  "Create a user without any authentication information which is
  assumed to have previously been authenticated. Once a user has been
  authenticated for a request, their authentication parameters should
  no longer be needed, so users are normalised into a model without
  these parameters."
  [email permissions]
  {:email (get-valid-email email) :permissions permissions})

(defn authenticated!
  "Asserts that the given user has been authenticated and returns a
  representation of the user without authentication information."
  [{:keys [email permissions] :as user}]
  (create-authenticated-user email permissions))

(defn try-authenticate
  "Tries to authenticate a user with the given candidate
  password. Return a representation of the authenticated user if
  successful, or nil if the authentication failed."
  [user submitted-password]
  (when (password-valid? user submitted-password)
    (authenticated! user)))

(defn get-summary
  "Returns a map containing summary information about a user."
  [{:keys [email] :as user}]
  {:username email})

(defn has-permission?
  "Check if a user has a given permission."
  [user permission]
  (contains? (:permissions user) permission))

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
        (create-authenticated-user email (role->permissions role))
        (user-token-invalid token :role "Unknown role")))
    (user-token-invalid token :email "Invalid address")))

(defn has-owner? [draftset]
  (contains? draftset :current-owner))

(defn is-owner? [user {:keys [current-owner] :as draftset}]
  (= (username user) current-owner))

(defn is-submitted-by? [user {:keys [submitted-by] :as draftset}]
  (= (username user) submitted-by))

(defn can-claim? [user {:keys [claim-permission claim-user] :as draftset}]
  (or (is-owner? user draftset)
      (and (not (has-owner? draftset))
           (is-submitted-by? user draftset))
      (and (some? claim-permission)
           (has-permission? user claim-permission))
      (= claim-user (username user))))

;; Currently the conditions for viewing a draft (in a list of all drafts, etc)
;; happen to be the same as the conditions for claiming a draft. This needn't
;; be the case in the future.
(def can-view? can-claim?)

(defn permitted-draftset-operations [draftset user]
  (cond
   (is-owner? user draftset)
   (set (keep #(when-let [[_ op] (re-matches #"draft:(.*)" (name %))]
                 (keyword op))
              (:permissions user)))

   (can-claim? user draftset)
   #{:claim}

   :else #{}))
