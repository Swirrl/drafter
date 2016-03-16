(ns drafter.user.mongo-test
  (:require [drafter.user.mongo :refer :all]
            [drafter.user.repository :refer :all]
            [drafter.user :as user]
            [clojure.test :refer :all]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.db :as db])
  (:import [org.bson.types ObjectId]))

(def ^:private ^:dynamic *user-repo*)

(deftest find-existing-user-test
  (let [email "test@example.com"
        test-user (user/create-user email :publisher "dsklfsjde")]
    (insert-user *user-repo* test-user)

    (let [found-user (find-user-by-username *user-repo* email)]
      (is (= test-user found-user)))))

(deftest get-all-users-test
  (let [role-f (fn [i] (get user/roles (mod i (count user/roles))))
        email-f #(str "user" % "@example.com")
        expected-users (map #(user/create-user (email-f %) (role-f %) (str %)) (range 1 10))]

    (doseq [u expected-users]
      (insert-user *user-repo* u))

    (let [actual-users (get-all-users *user-repo*)]
      (is (= (set expected-users) (set actual-users))))))

(deftest find-non-existent-user-test
  (is (nil? (find-user-by-username *user-repo* "missing@example.com"))))

(deftest find-malformed-user-record-test
  ;;user record is invalid - role number is out of range and
  ;;api_key_digest is missing
  (let [email  "foo@example.com"
        user-record {:_id (ObjectId.)
                     :email email
                     :role_number 18}]
    (insert-document *user-repo* user-record)

    (is (thrown? RuntimeException (find-user-by-username *user-repo* email)))))

(defn- with-clean-mongo-db [test-function]
  (let [conn (mg/connect)
        db-name "drafter_test"
        user-collection "Users"]
    (mg/drop-db conn db-name)
    (let [user-db (mg/get-db conn db-name)]
      (with-open [repo (->MongoUserRepository conn user-db user-collection)]
        (binding [*user-repo* repo]
          (test-function))))))

(use-fixtures :each with-clean-mongo-db)

