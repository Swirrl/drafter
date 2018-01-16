(ns drafter.feature.users.list-test
  (:require [drafter.feature.users.list :as sut]
            [clojure.test :as t]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]))

(defn- get-users-request
  "Issue a request as the given user."
  [user]
  (tc/with-identity user {:uri "/v1/users" :request-method :get}))

(tc/deftest-system-with-keys get-users-test [:drafter.feature.users.list/get-users-handler]
  [{:keys [:drafter.feature.users.list/get-users-handler]
    user-repo :drafter.user/memory-repository} "test-system.edn"]
  (let [users (user/get-all-users user-repo)
        expected-summaries (map user/get-summary users)
        {:keys [body] :as response} (get-users-handler (get-users-request test-editor))]
    (tc/assert-is-ok-response response)
    (t/is (= (set expected-summaries) (set body)))))


(tc/deftest-system-with-keys get-users-unauthenticated [:drafter.feature.users.list/get-users-handler]
  [{:keys [:drafter.feature.users.list/get-users-handler]
    user-repo :drafter.user/memory-repository} "test-system.edn"]
  (let [response (get-users-handler {:uri "/v1/users" :request-method :get})]
    (tc/assert-is-unauthorised-response response)))
