(ns drafter.stasher.cache-clearer-test
  (:require [drafter.stasher.cache-clearer :as sut]
            [clojure.test :as t]))

#_(t/deftest closest-test

    (t/is (= [5 4 4 2 1]
             (sut/sorted-by-distance 5 [1 5 2 4 4])))
  
    (t/are [target expected items] (= expected (first (sut/sorted-by-distance target items)))
      5 5 [1 2 3 4 5 6 7 8 9 0]
      5 4 [1 2 2 4 9 8]
      5 2 [10 11 12 13 14 2]
      5 10 [10 10 10 10 10 10]
      10 nil []))
