(ns drafter.user.memory-repository-test
  (:require [clojure.test :refer :all]
            [drafter.test-common :as tc]
            [drafter.user :as user :refer [create-user get-digest roles username]]
            [drafter.user-test :refer [test-editor]]
            [drafter.user.memory-repository :refer :all]))

(use-fixtures :each tc/with-spec-instrumentation)

(deftest find-existing-user-test
  (let [repo (init-repository* test-editor)
        u (user/find-user-by-username repo (username test-editor))]
    (is (= test-editor u))))

(deftest find-non-existent-user-test
  (let [repo (init-repository*)
        u (user/find-user-by-username repo "missing@example.com")]
    (is (nil? u))))

(deftest get-all-users-test
  (let [email-f #(str (name %) "@example.com")
        users (map #(create-user (email-f %) % (get-digest "password")) roles)
        repo (init-repository users)
        found-users (user/get-all-users repo)]
    (is (= (set users) (set found-users)))))

(deftest add-user-test
  (let [repo (init-repository*)]
    (add-user repo test-editor)
    (let [found (user/find-user-by-username repo (username test-editor))]
      (is (= test-editor found)))))
