(ns drafter.user
  (:require
   [clojure.set :as set]
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

(defrecord User [email permissions password-digest])

(defmethod ig/init-key :drafter.user/repo [k opts]
  (throw (ex-info "Config error.  Please use a concrete implementation of the user repo instead." {})))

(defn role->permissions
  "This function is only intended to be used for compatibility with mongo user
   storage, or for convenience in tests."
  [role]
  (case role
    :norole #{}
    :access #{:drafter:public:view}
    :editor (conj (role->permissions :access)
                  :drafter:draft:claim :drafter:draft:create
                  :drafter:draft:delete :drafter:draft:edit
                  :drafter:draft:submit :drafter:draft:share
                  :drafter:draft:view :drafter:job:view :drafter:user:view)
    :publisher (conj (role->permissions :editor) :drafter:draft:publish)
    ;; :manager is used in tests to demonstrate scoped claim permissions.
    :manager (conj (role->permissions :publisher) :drafter:draft:claim:manager)
    :system (recur :manager)))

(defn ^{:deprecated "For backward compatibility only"} permissions->role
  "This is a shim to provide a role in the API when we only have permissions
   internally. Deprecated and only to be used for backward compatibility."
  [permissions]
  (first (filter (fn [role] (set/subset? (role->permissions role) permissions))
                 [:manager :publisher :editor :access :norole])))

(def ^{:deprecated "For backward compatibility only"} role->canonical-permission
  "Maps a role to a permission that that role has, but less privileged roles
   don't have. Deprecated, for backward compatibility only."
  {"editor" "drafter:draft:edit"
   "publisher" "drafter:draft:publish"
   "manager" "drafter:draft:claim:manager"})

(def ^{:deprecated "For backward compatibility only"} canonical-permission->role
  "Deprecated, for backward compatibility only."
  (set/map-invert role->canonical-permission))

(def permission-summary
  {:drafter:draft:claim "Claim submitted drafts"
   :drafter:draft:create "Create drafts"
   :drafter:draft:delete "Delete drafts"
   :drafter:draft:edit "Edit drafts"
   :drafter:draft:publish "Publish drafts"
   :drafter:draft:share "Share drafts to be viewed"
   :drafter:draft:submit "Submit drafts to be claimed"
   :drafter:draft:view "View shared drafts"
   :drafter:job:view "View the status of jobs"
   :drafter:public:view "View the public endpoint (if global auth is on)"
   :drafter:user:view "View users"})

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
  [user]
  {:username (:email user)
   :role (permissions->role (:permissions user))})

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

(defn can-view? [user draftset]
  (or (can-claim? user draftset)
      (contains? (:view-users draftset) (username user))
      (some #(has-permission? user %) (:view-permissions draftset))))

(defn permitted-draftset-operations [draftset user]
  (cond
   (is-owner? user draftset)
   (set (keep #(when-let [[_ op] (re-matches #"drafter:draft:(.*)" (name %))]
                 (keyword op))
              (:permissions user)))

   (can-claim? user draftset)
   #{:claim}

   :else #{}))
