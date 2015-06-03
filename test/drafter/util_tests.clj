(ns drafter.util-tests
  (:require [drafter.util :refer :all]
            [clojure.test :refer :all]))

(deftest get-causes-test
  (testing "Returns all causes"
    (let [innermost (RuntimeException. "!!!")
          middle (IllegalStateException. ":'(" innermost)
          outer (Exception. "noooooo" middle)]
      (is (= [outer middle innermost] (get-causes outer))))))

(deftest unfold-test
  (testing "Gets pairs"
    (let [f (fn [x] (if (< x 10) [x (+ 2 x)]))]
      (is (= [0 2 4 6 8] (unfold f 0)))))

  (testing "Empty sequence if function returns nil"
    (is (= nil (unfold (constantly nil) 3)))))
