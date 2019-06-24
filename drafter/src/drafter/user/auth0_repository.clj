(ns drafter.user.auth0-repository
  (:require [swirrl.auth0.client :as auth0]
            [clojure.tools.logging :as log]
            [drafter.user :refer [username UserRepository]]
            [integrant.core :as ig]))

(defn -find-user-by-username [auth0 email]
  (log/info "getting " email " from Auth0")
  (-> auth0
      (auth0/api :users-by-email {:email email})
      (first)))

(defrecord Auth0UserRepository [auth0]
  UserRepository
  (find-user-by-username [this username] (-find-user-by-username auth0 username))
  (get-all-users [this]
    (throw (UnsupportedOperationException. "Can't list users from Auth0"))))

(derive :drafter.user/auth0-repository :drafter.user/repo)

(defmethod ig/init-key :drafter.user/auth0-repository
  [_ {:keys [auth0] :as opts}]
  (->Auth0UserRepository auth0))
