(ns drafter.user.mongo
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [schema.core :as s]
            [drafter.user :as user]
            [drafter.user.repository :refer :all]
            [clojure.set :as set])
  (:import [java.io Closeable]
           [org.bson.types ObjectId]))

(def ^:private role-mappings
  {10 :editor
   20 :publisher
   30 :manager
   40 :system})

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
  (find-user-by-username [this username]
    (if-let [mongo-user (mc/find-one-as-map db user-collection {:email username})]
      (mongo-user->user mongo-user)))

  (get-all-users [this]
    (->>  user-collection
          (mc/find-maps db)
          (map mongo-user->user)))

  Closeable
  (close [this]
    (mg/disconnect conn)))

(defn get-repository [env-map]
  (let [conn (mg/connect)
        db (mg/get-db conn "pmd-host_development")]
    (->MongoUserRepository conn db "publish_my_data_users")))

(defn- user->mongo-user [user]
  (let [[email role digest] ((juxt user/username user/role user/password-digest) user)
        role-number (get (set/map-invert role-mappings) role)]
    {:_id (ObjectId.)
     :email email
     :role_number role-number
     :api_key_digest digest}))

(defn insert-document
  "Inserts a map into the user collection referenced by a mongo user
  repository."
  [{:keys [db user-collection] :as repo} document]
  (mc/insert db user-collection document))

(defn insert-user
  "Inserts a user into a mongo db user repository."
  [repo user]
  (let [mongo-user (user->mongo-user user)]
    (insert-document repo mongo-user)))
