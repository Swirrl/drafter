(ns drafter.user.auth0-repository-test
  (:require [clojure.test :refer :all]
            [drafter.test-common :as tc]
            [drafter.user :as user :refer [create-user get-digest roles username]]
            [drafter.user-test :refer [test-editor]]
            [drafter.user.auth0-repository :as repo]))

(use-fixtures :each tc/with-spec-instrumentation)

(tc/deftest-system-with-keys find-existing-user-test
  [:drafter.user/auth0-repository]
  [{repo :drafter.user/auth0-repository} "auth0-repo.edn"]
  (let [u (user/find-user-by-username repo (username test-editor))]
    (is (= (:email test-editor) (:email u)))))

(tc/deftest-system-with-keys find-non-existent-user-test
  [:drafter.user/auth0-repository]
  [{repo :drafter.user/auth0-repository} "auth0-repo.edn"]
  (let [u (user/find-user-by-username repo "missing@example.com")]
    (is (nil? u))))
