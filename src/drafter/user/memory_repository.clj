(ns drafter.user.memory-repository
  (:require [clojure.edn :as edn]
            [integrant.core :as ig]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [drafter.user :refer [create-user get-digest username]]
            [drafter.user.repository :refer :all])
  (:import [java.io Closeable FileNotFoundException PushbackReader]))

(defrecord MemoryUserRepository [users]
  UserRepository
  (find-user-by-username [this username] (get @users username))
  (get-all-users [this] (vals @users))

  Closeable
  (close [this]))

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

(defn get-repository [config]
  (log/info "Creating memory repository")
  (try
    (create-repository-from-file "test-users.edn")
    (catch FileNotFoundException e
      (let [msg "To use the memory repository you must have a test-users.edn file configured.  Aborting."]
        (println msg)
        (log/fatal msg))
      (throw e))))

(defmethod ig/init-key :drafter.user/memory-repository [k opts]
  (get-repository opts))
