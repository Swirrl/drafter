(ns drafter.user.mongo-test
  (:require [drafter.user.mongo :refer :all]
            [drafter.user.repository :refer :all]
            [clojure.test :refer :all]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.db :as db])
  (:import [org.bson.types ObjectId]))

(def ^:private ^:dynamic *user-db*)
(def ^:private ^:dynamic *user-repo*)
(def ^:private ^:dynamic *user-collection*)

(deftest find-existing-user-test
  (let [email "test@example.com"
        api-key-digest "sdlfksflksd"
        user-record {:_id (ObjectId.)
                     :email email
                     :role_number 20
                     :api_key_digest api-key-digest}]
    (mc/insert *user-db* *user-collection* user-record)

    (let [user (find-user-by-username *user-repo* email)]
      (is (= email (:email user)))
      (is (= :publisher (:role user)))
      (is (= api-key-digest (:api-key-digest user))))))

(deftest find-non-existent-user-test
  (is (nil? (find-user-by-username *user-repo* "missing@example.com"))))

(deftest find-malformed-user-record-test
  ;;user record is invalid - role number is out of range and
  ;;api_key_digest is missing
  (let [email  "foo@example.com"
        user-record {:_id (ObjectId.)
                     :email email
                     :role_number 18}]
    (mc/insert *user-db* *user-collection* user-record)

    (is (thrown? RuntimeException (find-user-by-username *user-repo* email)))))

(defn- with-clean-mongo-db [test-function]
  (let [conn (mg/connect)
        db-name "drafter_test"
        user-collection "Users"]
    (mg/drop-db conn db-name)
    (let [user-db (mg/get-db conn db-name)]
      (with-open [repo (->MongoUserRepository conn user-db user-collection)]
        (binding [*user-db* user-db
                  *user-collection* user-collection
                  *user-repo* repo]
          (test-function))))))

(use-fixtures :each with-clean-mongo-db)

