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
  (are [query-timeout user-timeout endpoint-timeout expected] (= expected (calculate-query-timeout query-timeout user-timeout endpoint-timeout))
       nil nil nil default-query-timeout                    ;;no timeout set
       10 nil nil 10                                        ;;query timeout only
       nil 5 nil 5                                          ;;user timeout only
       nil nil 20 20                                        ;;endpoint timeout only
       5 10 nil 5                                           ;;query timeout < user timeout
       10 8 nil 8                                           ;;query timeout > user timeout
       3 nil 10 3                                           ;;query timeout < endpoint timeout
       10 nil 5 5                                           ;;query timeout > endpoint timeout
       nil 3 5 3                                            ;;user timeout < endpoint timeout
       nil 10 5 10                                          ;;user timeout > endpoint timeout
       2 10 5 2                                             ;;query timeout < (max user-timeout endpoint-timeout)
       10 5 nil 5                                           ;;query timeout > (max user-timeout endpoint-timeout)

       100 10 5 10                                          ;;query timeout > user-timeout > endpoint-timeout
       100 5 10 5                                           ;;query timeout > endpoint-timeout > user-timeout
       ))
