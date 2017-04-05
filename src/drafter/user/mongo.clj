(ns drafter.user.mongo
  (:require [clojure
             [set :as set]
             [string :as string]]
            [drafter.user :as user]
            [drafter.user.repository :refer :all]
            [monger
             [collection :as mc]
             [core :as mg]]
            [schema.core :as s])
  (:import java.io.Closeable
           org.bson.types.ObjectId))

(def ^:private role-mappings
  {10 :editor
   20 :publisher
   30 :manager
   40 :system})

(def ^:private mongo-user-schema
  {(s/required-key :email) s/Str
   (s/required-key :role_number) (apply s/enum (keys role-mappings))
   (s/required-key :encrypted_password) s/Str
   s/Any s/Any})

(defn- mongo-user->user [{:keys [role_number email encrypted_password] :as mongo-user}]
  (s/validate mongo-user-schema mongo-user)
  (user/create-user email (role-mappings role_number) encrypted_password))

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

(defn- get-mongo-port [port-str]
  (try
    (Long/parseLong port-str)
    (catch NumberFormatException ex
      (throw (ex-info "Invalid format for mongo port" {} ex)))))

(defn- env-key->env-var [key]
  (string/upper-case (string/replace (name key) #"-" "_")))

(defn- missing-config-key-exception
  ([msg key] (missing-config-key-exception msg key nil))
  ([msg key cause]
    (let [ex-msg (str msg ". Specify the environment variable: " (env-key->env-var key))]
      (ex-info ex-msg {:missing-key key} cause))))

(defn- get-host-config [env-map]
  (let [host-key :drafter-mongo-host
        port-key :drafter-mongo-port
        [host port] (map env-map [host-key port-key])]
    (cond (and (some? host) (some? port))
          {:host host :port (get-mongo-port port)}

          (and (nil? host) (nil? port))
          []

          (and (some? host) (nil? port))
          (throw (missing-config-key-exception "Mongo port required when host specified" port-key))

          :else (throw (missing-config-key-exception "Mongo host required when port specified" host-key)))))

(defn- get-user-db-name [env-map]
  (if-let [user-db-name (:drafter-user-db-name env-map)]
    user-db-name
    (throw (missing-config-key-exception "User database name required" :drafter-user-db-name))))

(defn get-repository [env-map]
  (let [host-config (get-host-config env-map)
        db-name (get-user-db-name env-map)
        conn (if (nil? host-config) (mg/connect) (mg/connect host-config))
        db (mg/get-db conn db-name)]
    (->MongoUserRepository conn db "publish_my_data_users")))

(defn- user->mongo-user [user]
  (let [[email role digest] ((juxt user/username user/role user/password-digest) user)
        role-number (get (set/map-invert role-mappings) role)]
    {:_id (ObjectId.)
     :email email
     :role_number role-number
     :encrypted_password digest}))

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
