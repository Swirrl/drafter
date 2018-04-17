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

(t/deftest centile-boundaries-test
  (t/is (= {[0 5] [0 4.05],
            [5 75] [4.05 74.75],
            [75 100] [74.75 99.0]} (sut/centile-boundaries [5 75 100]
                                                           (range 100)))))

(t/deftest within-threshold?-test
  (t/are [quota threshold input expected]
      (= expected (if (sut/within-threshold? quota threshold input)
                    true
                    false))
    ;; within 10% of target quota
    100 0.1 100 true
    100 0.1 90 true

    100 0.1 91 true
    100 0.1 110 true

    ;; outside 10% of target quota
    100 0.1 89 false
    100 0.1 111 false

    100 0.1 1110000 false

    ;; within 100% of target
    100 1.0 200 true
    100 1.0 0 true

    ;; outside 100% of target
    100 1.0 -1 false
    100 1.0 201 false

    20000000 1.0 9999 false

    
    
    ))
