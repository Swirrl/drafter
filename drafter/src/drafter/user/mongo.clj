(ns drafter.user.mongo
  (:require [clojure.set :as set]
            [drafter.user :as user]
            [monger
             [collection :as mc]
             [core :as mg]]
            [clojure.spec.alpha :as s]
            [integrant.core :as ig])
  (:import java.io.Closeable
           org.bson.types.ObjectId))

(def ^:private role-mappings
  {00 :access
   10 :editor
   20 :publisher
   30 :manager
   40 :system})

(s/def :mongo/email string?)
(s/def :mongo/role_number #(contains? role-mappings %))
(s/def :mongo/encrypted_password string?)
(s/def ::MongoUserSchema (s/keys :req-un [:mongo/email :mongo/role_number :mongo/encrypted_password]))

(defn- validate-spec! [spec x]
  (when-not (s/valid? spec x)
    (throw (ex-info "Object did not satisfy spec" {:spec spec :object x}))))

(defn- mongo-user->user [{:keys [role_number email encrypted_password] :as mongo-user}]
  ;;TODO: check with instrumentation?
  (validate-spec! ::MongoUserSchema mongo-user)
  (user/create-user email (role-mappings role_number) encrypted_password))

(defrecord MongoUserRepository [conn db user-collection]
  user/UserRepository
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

(defn- missing-config-key-exception
  ([msg key] (missing-config-key-exception msg key nil))
  ([msg key cause]
    (let [ex-msg (str msg ". Specify the configuration setting: " (name key))]
      (ex-info ex-msg {:missing-key key} cause))))

(defn- get-host-config [{:keys [mongo-host mongo-port]}]
  (cond (and (some? mongo-host) (some? mongo-port))
        {:host mongo-host :port mongo-port}

        (and (nil? mongo-host) (nil? mongo-port))
        []

        (and (some? mongo-host) (nil? mongo-port))
        (throw (missing-config-key-exception "Mongo port required when host specified" :mongo-port))

        :else (throw (missing-config-key-exception "Mongo host required when port specified" :mongo-host))))

(defn- get-user-db-name [config]
  (if-let [user-db-name (:mongo-db-name config)]
    user-db-name
    (throw (missing-config-key-exception "User database name required" :mongo-db-name))))

(defn- get-user-collection-name [config]
  ;;NOTE: user collection has default if not explicitly configured so should always exist
  (:mongo-user-collection config))

(defn get-repository [config]
  (let [host-config (get-host-config config)
        db-name (get-user-db-name config)
        collection-name (get-user-collection-name config)
        conn (if (nil? host-config) (mg/connect) (mg/connect host-config))
        db (mg/get-db conn db-name)]
    (->MongoUserRepository conn db collection-name)))

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

(derive :drafter.user/mongo :drafter.user/repo)

(defmethod ig/init-key :drafter.user/mongo [k opts]
  ;; merge the config options onto the record for convenience
  (merge (get-repository opts) opts))

(defmethod ig/halt-key! :drafter.user/mongo [_ mongo]
  (.close mongo))
