(ns drafter.user.memory-repository
  (:require [clojure.edn :as edn]
            [integrant.core :as ig]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [drafter.user :refer [create-user get-digest username UserRepository]])
  (:import [java.io Closeable FileNotFoundException PushbackReader]))

(defn -find-user-by-username [users username]
  (log/info "getting " username " from " users)
  (get @users username))

(defrecord MemoryUserRepository [users]
  UserRepository
  (find-user-by-username [this username] (-find-user-by-username users username))
  (get-all-users [this] (vals @users)))

(defn init-repository [users]
  (let [user-map (reduce (fn [m user] (assoc m (username user) user)) {} users)]
    (->MemoryUserRepository (atom user-map))))

(defn init-repository* [& users]
  (init-repository users))

(defn add-user
  "Adds a user to this repository."
  [{:keys [users] :as repo} user]
  (swap! users (fn [m u] (assoc m (username u) u)) user))

(defn- user-decl->user [{:keys [username password role]}]
  (create-user username role (get-digest password)))

(defn create-repository
  "Given a seq of users and their plaintext passwords generate hashed
  passwords for them and return them in a memory repository."
  [users]
  (let [repo (init-repository*)]
    (doseq [user-decl users]
      (add-user repo (user-decl->user user-decl)))
    repo))

(derive :drafter.user/memory-repository :drafter.user/repo)

(defmethod ig/init-key :drafter.user/memory-repository [k {:keys [users] :as opts}]
  (merge (create-repository users) (dissoc opts :users)))

