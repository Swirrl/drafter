(ns drafter.user.mongo
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [schema.core :as s]
            [drafter.user :as user]
            [drafter.user.repository :refer :all])
  (:import [java.io Closeable]))

(def ^:private role-mappings
  {10 :editor
   20 :publisher
   30 :manager})

(def ^:private mongo-user-schema
  {(s/required-key :email) s/Str
   (s/required-key :role_number) (apply s/enum (keys role-mappings))
   (s/required-key :api_key_digest) s/Str
   s/Any s/Any})

(defn- mongo-user->user [{:keys [role_number email api_key_digest] :as mongo-user}]
  (s/validate mongo-user-schema mongo-user)
  (user/create-user email (role-mappings role_number) api_key_digest))

(defrecord MongoUserRepository [conn db user-collection]
  UserRepository
  (find-user-by-email-address [this email]
    (if-let [mongo-user (mc/find-one-as-map db user-collection {:email email})]
      (mongo-user->user mongo-user)))

  Closeable
  (close [this]
    (mg/disconnect conn)))

(defn create-repository [env-map]
  (let [conn (mg/connect)
        db (mg/get-db conn "pmd-host_development")]
    (->MongoUserRepository conn db "publish_my_data_users")))
