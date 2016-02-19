(ns drafter.user.memory-repository
  (:require [drafter.user :refer [username]]
            [drafter.user.repository :refer :all]))

(defrecord MemoryUserRepository [users]
  UserRepository
  (find-user-by-email-address [this email] (get @users email)))

(defn create-repository [users]
  (let [user-map (reduce (fn [m user] (assoc m (username user) user)) {} users)]
    (->MemoryUserRepository (atom user-map))))

(defn create-repository* [& users]
  (create-repository users))

(defn add-user
  "Adds a user to this repository."
  [{:keys [users] :as repo} user]
  (swap! users (fn [m u] (assoc m (username u) u)) user))

(defn get-repository [env-map]
  (create-repository*))
