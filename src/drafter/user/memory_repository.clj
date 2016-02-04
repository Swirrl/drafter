(ns drafter.user.memory-repository
  (:require [drafter.user.repository :refer :all]))

(defrecord MemoryUserRepository [user-map]
  UserRepository
  (find-user-by-email-address [this email] (get user-map email)))

(defn create-repository [users]
  (let [user-map (reduce (fn [m {email :email :as user}] (assoc m email user)) {} users)]
    (->MemoryUserRepository user-map)))

(defn create-repository* [& users]
  (create-repository users))
