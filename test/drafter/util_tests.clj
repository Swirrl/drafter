(ns drafter.util-tests
  (:require [drafter.util :refer :all]
            [clojure.test :refer :all]))

(deftest get-causes-test
  (testing "Returns all causes"
    (let [innermost (RuntimeException. "!!!")
          middle (IllegalStateException. ":'(" innermost)
          outer (Exception. "noooooo" middle)]
      (is (= [outer middle innermost] (get-causes outer))))))

(deftest to-coll-test
  (are [coll expected] (= coll expected)
       (to-coll 1) [1]
       (to-coll [1 2]) [1 2]
       (to-coll 3 hash-set) #{3}
       (to-coll [1 2] hash-set) [1 2]
       (to-coll nil) nil))
