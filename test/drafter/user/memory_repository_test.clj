(ns drafter.user.memory-repository-test
  (:require [clojure.test :refer :all]
            [drafter
             [user :refer [create-user get-digest roles username]]
             [user-test :refer [test-editor]]]
            [drafter.user
             [memory-repository :refer :all]
             [repository :refer [find-user-by-username get-all-users]]]))

(deftest find-existing-user-test
  (let [repo (create-repository* test-editor)
        u (find-user-by-username repo (username test-editor))]
    (is (= test-editor u))))

(deftest find-non-existent-user-test
  (let [repo (create-repository*)
        u (find-user-by-username repo "missing@example.com")]
    (is (nil? u))))

(deftest get-all-users-test
  (let [email-f #(str (name %) "@example.com")
        users (map #(create-user (email-f %) % (get-digest "password")) roles)
        repo (create-repository users)
        found-users (get-all-users repo)]
    (is (= (set users) (set found-users)))))

(deftest add-user-test
  (let [repo (create-repository*)]
    (add-user repo test-editor)
    (let [found (find-user-by-username repo (username test-editor))]
      (is (= test-editor found)))))
