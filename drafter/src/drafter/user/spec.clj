(ns drafter.user.spec
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [drafter.user :as user]
            [drafter.draftset :as ds]
            [drafter.draftset.spec]
            [drafter.spec]
            [clojure.spec.test.alpha :as st])
  (:import [drafter.user User]))

(s/def ::user/role user/roles)
(s/def ::user/permissions (s/coll-of keyword? :kind set?))
(s/def ::user/email :drafter/EmailAddress)
(s/def ::user/password-digest string?)
(s/def ::user/username :drafter/EmailAddress)

(s/def :token/email string?)
(s/def :token/role string?)

(s/def ::user/DbUser #(instance? User %))
(s/def ::user/User
  (s/keys :req-un [::user/email ::user/permissions]))
(s/def ::user/UserSummary
  (s/keys :req-un [::user/username]))
(s/def ::user/UserToken (s/keys :req-un [:token/email :token/role]))

(s/def ::user/repo #(satisfies? user/UserRepository %))

(s/fdef user/get-digest
  :args (s/cat :passwd-str string?)
  :ret string?)

(s/fdef user/password-valid?
  :args (s/cat :user ::user/DbUser :submitted-key string?)
  :ret boolean?)

(s/fdef user/username->uri
  :args (s/cat :username ::user/email)
  :ret :drafter/URI)

(s/fdef user/create-user
  :args (s/cat :email string?
               :role ::user/role
               :password-digest ::user/password-digest)
  :ret ::user/DbUser)

(s/fdef user/create-authenticated-user
  :args (s/cat :email string? :permissions ::user/permissions)
  :ret ::user/User)

(s/fdef user/authenticated!
  :args (s/cat :user ::user/User)
  :ret ::user/User)

(s/fdef user/get-summary
  :args (s/cat :user ::user/User)
  :ret ::user/UserSummary)

(s/fdef user/validate-token!
  :args (s/cat :token ::user/UserToken)
  :ret ::user/User)

(s/fdef user/has-owner?
  :args (s/cat :draftset ::ds/Draftset)
  :ret boolean?)

(s/fdef user/is-owner?
  :args (s/cat :user ::user/User :draftset ::ds/Draftset)
  :ret boolean?)

(s/fdef user/can-claim?
  :args (s/cat :user ::user/User :draftset ::ds/Draftset)
  :ret boolean?)

(s/fdef user/permitted-draftset-operations
  :args (s/cat :draftset ::ds/Draftset :user ::user/User)
  :ret (s/coll-of ::ds/Operation :kind set?))

(def ^:private fns-with-specs
  [`user/get-digest
   `user/password-valid?
   `user/username->uri
   `user/create-user
   `user/create-authenticated-user
   `user/authenticated!
   `user/get-summary
   `user/validate-token!
   `user/has-owner?
   `user/is-owner?
   `user/can-claim?
   `user/permitted-draftset-operations])

(defn instrument []
  (st/instrument fns-with-specs))

(defn unstrument []
  (st/instrument fns-with-specs))
