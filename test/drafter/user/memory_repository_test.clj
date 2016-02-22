(ns drafter.user.memory-repository-test
  (:require [clojure.test :refer :all]
            [drafter.user.memory-repository :refer :all]
            [drafter.user :refer [create-user username test-editor]]
            [drafter.user.repository :refer [find-user-by-username]]))

(deftest find-existing-user-test
  (let [repo (create-repository* test-editor)
        u (find-user-by-username repo (username test-editor))]
    (is (= test-editor u))))

(deftest find-non-existent-user-test
  (let [repo (create-repository*)
        u (find-user-by-username repo "missing@example.com")]
    (is (nil? u))))

(deftest add-user-test
  (let [repo (create-repository*)]
    (add-user repo test-editor)
    (let [found (find-user-by-username repo (username test-editor))]
      (is (= test-editor found)))))
