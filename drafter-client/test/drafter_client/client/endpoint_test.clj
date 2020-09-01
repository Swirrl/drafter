(ns drafter-client.client.endpoint-test
  (:require [clojure.test :refer :all]
            [drafter-client.client.endpoint :refer :all :as endpoint]
            [drafter-client.client.draftset]
            [clj-time.core :as t])
  (:import [java.util UUID]))

(deftest from-json-test
  (testing "Endpoint"
    (let [id "id"
          created-at (t/minus (t/now) (t/days 2))
          updated-at (t/plus created-at (t/hours 6))
          json {:id id
                :type "Endpoint"
                :created-at (str created-at)
                :updated-at (str updated-at)}
          result (from-json json)]
      (is (= id (endpoint-id result)))
      (is (= created-at (endpoint/created-at result)))
      (is (= updated-at (endpoint/updated-at result)))))

  (testing "Draftset"
    (let [id (UUID/randomUUID)
          created-at (t/minus (t/now) (t/days 2))
          updated-at (t/plus created-at (t/hours 6))
          json {:id (str id)
                :type "Draftset"
                :created-at (str created-at)
                :updated-at (str updated-at)
                :display-name "Display name"
                :description "Description"}
          result (from-json json)]
      (is (= id (endpoint-id result)))
      (is (= created-at (endpoint/created-at result)))
      (is (= updated-at (endpoint/updated-at result))))))
