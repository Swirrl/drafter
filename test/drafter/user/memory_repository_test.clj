(ns drafter.user.memory-repository-test
  (:require [clojure.test :refer :all]
            [drafter.user.memory-repository :refer :all]
            [drafter.user :refer [create-user username]]
            [drafter.user.repository :refer [find-user-by-email-address]]
            [drafter.test-common :refer [test-editor]]))

(deftest find-existing-user-test
  (let [repo (create-repository* test-editor)
        u (find-user-by-email-address repo (username test-editor))]
    (is (= test-editor u))))

(deftest find-non-existent-user-test
  (let [repo (create-repository*)
        u (find-user-by-email-address repo "missing@example.com")]
    (is (nil? u))))

(deftest add-user-test
  (let [repo (create-repository*)]
    (add-user repo test-editor)
    (let [found (find-user-by-email-address repo (username test-editor))]
      (is (= test-editor found)))))
