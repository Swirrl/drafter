(ns drafter.util-tests
  (:require [drafter.util :refer :all]
            [clojure.test :refer :all]))

(deftest get-causes-test
  (testing "Returns all causes"
    (let [innermost (RuntimeException. "!!!")
          middle (IllegalStateException. ":'(" innermost)
          outer (Exception. "noooooo" middle)]
      (is (= [outer middle innermost] (get-causes outer))))))
