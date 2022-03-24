(ns drafter.user.mongo-test
  (:require [clojure.test :refer :all]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user.mongo :as um]
            [monger.core :as mg])
  (:import org.bson.types.ObjectId))

(def ^:private ^:dynamic *user-repo*)

(defn- with-clean-mongo-db [test-function]
  (let [conn (mg/connect)
        db-name "drafter_test"
        user-collection "Users"]
    (mg/drop-db conn db-name)
    (let [user-db (mg/get-db conn db-name)]
      (with-open [repo (um/->MongoUserRepository conn user-db user-collection)]
        (binding [*user-repo* repo]
          (test-function))))))

(use-fixtures :each  with-clean-mongo-db tc/with-spec-instrumentation)



(deftest find-existing-user-test
  (let [email "test@example.com"
        test-user (user/create-user email :publisher "dsklfsjde")]
    (um/insert-user *user-repo* test-user)

    (let [found-user (user/find-user-by-username *user-repo* email)]
      (is (= test-user found-user)))))

(deftest get-all-users-test
  (let [email-f #(str "user" % "@example.com")
        expected-users (map #(user/create-user (email-f %1) %2 (str %1))
                            (range 1 10)
                            (cycle user/roles))]

    (doseq [u expected-users]
      (um/insert-user *user-repo* u))

    (let [actual-users (user/get-all-users *user-repo*)]
      (is (= (set expected-users) (set actual-users))))))

(deftest find-non-existent-user-test
  (is (nil? (user/find-user-by-username *user-repo* "missing@example.com"))))

(deftest find-malformed-user-record-test
  ;;user record is invalid - role number is out of range and
  ;;api_key_digest is missing
  (let [email  "foo@example.com"
        user-record {:_id (ObjectId.)
                     :email email
                     :role_number 18}]
    (um/insert-document *user-repo* user-record)

    (is (thrown? RuntimeException (user/find-user-by-username *user-repo* email)))))
