(ns drafter.routes.draftsets-api.get-draftsets-test
  (:require [clojure.test :refer [is testing]]
            [drafter.draftset :refer [Draftset]]
            [drafter.routes.draftsets-api :as sut :refer :all]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [drafter.routes.draftsets-api-test :as dat]
            [drafter.test-common :as tc]))

(tc/deftest-system-with-keys get-draftsets-handler-test [::sut/get-draftsets-handler :drafter/backend :drafter.fixture-data/loader]
  [{:keys [:drafter/backend ::sut/get-draftsets-handler] :as sys} "drafter/routes/draftsets-api/get-draftsets-handler.edn"]

  (let [get-draftsets-through-api (fn [include user]
                                    (let [request (dat/get-draftsets-request include user)
                                          {:keys [body] :as response} (get-draftsets-handler request)]
                                      (dat/ok-response->typed-body [Draftset] response)
                                      body))]

    (testing "All draftsets"
      (let [all-draftsets (get-draftsets-through-api :all test-publisher)]
        (is (= 2 (count all-draftsets)))
        (is (= #{"owned" "claimable"} (set (map :display-name all-draftsets))))))

    (testing "Missing include filter should return all owned and claimable draftsets"
      (let [request (tc/with-identity test-publisher {:uri "/v1/draftsets" :request-method :get})
            response (get-draftsets-handler request)
            draftsets (dat/ok-response->typed-body [Draftset] response)]
        (is (= 2 (count draftsets)))
        (is (= #{"owned" "claimable"} (set (map :display-name draftsets))))))

    (testing "Owned draftsets"
      (let [draftsets (get-draftsets-through-api :owned test-publisher)]
        (is (= 1 (count draftsets)))
        (is (= "owned" (:display-name (first draftsets))))))

    (testing "Claimable draftsets"
      (let [draftsets (get-draftsets-through-api :claimable test-publisher)]
        (is (= 1 (count draftsets)))
        (is (= "claimable" (:display-name (first draftsets))))))

    (testing "Invalid include parameter"
      (let [request (dat/get-draftsets-request :invalid test-publisher)
            response (get-draftsets-handler request)]
        (tc/assert-is-unprocessable-response response)))

    (testing "Unauthenticated"
      (let [response (get-draftsets-handler {:uri "/v1/draftsets" :request-method :get})]
        (tc/assert-is-unauthorised-response response)))))
