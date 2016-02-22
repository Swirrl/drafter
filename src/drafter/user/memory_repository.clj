(ns drafter.user.memory-repository
  (:require [drafter.user :refer [username create-user get-digest test-editor test-publisher test-manager]]
            [drafter.user.repository :refer :all]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log])
  (:import [java.io PushbackReader FileNotFoundException]))

(defrecord MemoryUserRepository [users]
  UserRepository
  (find-user-by-username [this username] (get @users username)))

(defn create-repository [users]
  (let [user-map (reduce (fn [m user] (assoc m (username user) user)) {} users)]
    (->MemoryUserRepository (atom user-map))))

(defn create-repository* [& users]
  (create-repository users))

(defn add-user
  "Adds a user to this repository."
  [{:keys [users] :as repo} user]
  (swap! users (fn [m u] (assoc m (username u) u)) user))

(defn- user-decl->user [{:keys [username password role]}]
  (create-user username role (get-digest password)))

(defn create-repository-from-file [source]
  (with-open [reader (PushbackReader. (io/reader source))]
    (let [repo (create-repository*)]
      (doseq [decl (edn/read reader)]
        (add-user repo (user-decl->user decl)))
      repo)))

(defn- default-user-repo []
  (create-repository* test-editor test-publisher test-manager))

(defn get-repository [env-map]
  (log/info "Creating memory repository")
  (try
    (create-repository-from-file "test-users.edn")
    (catch FileNotFoundException e
      (log/warn "test-users.edn does not exist - using default users")
      (default-user-repo))))
