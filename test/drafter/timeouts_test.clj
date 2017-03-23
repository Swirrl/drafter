(ns drafter.timeouts-test
  (:require [drafter.timeouts :refer :all]
            [clojure.test :refer :all]))

(deftest try-parse-timeout-test
  (testing "non-numeric timeouts invalid"
    (is (instance? Exception (try-parse-timeout "abc"))))

  (testing "negative timeouts invalid"
    (is (instance? Exception (try-parse-timeout "-22"))))

  (testing "should convert valid timeout"
    (is (= 34 (try-parse-timeout "34")))))

(deftest calculate-query-timeout-test
  (are [user-timeout endpoint-timeout expected] (= expected (calculate-query-timeout user-timeout endpoint-timeout))
       nil nil default-query-timeout
       10  nil 10
       nil 5   5
       10  5   10
       3   5   5))
