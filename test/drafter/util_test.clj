(ns drafter.util-test
  (:require [drafter.util :refer :all]
            [clojure.test :refer :all]
            [clojure.math.combinatorics :refer [permutations]]))

(deftest get-causes-test
  (testing "Returns all causes"
    (let [innermost (RuntimeException. "!!!")
          middle (IllegalStateException. ":'(" innermost)
          outer (Exception. "noooooo" middle)]
      (is (= [outer middle innermost] (get-causes outer))))))

(deftest conf-if-test
  (are [test col item expected] (= expected (conj-if test col item))
       true [] 1 [1]
       false [] 1 []))

(deftest batch-partition-by-test
  (are [seq partition-fn output-batch-size take-batch-size expected]
    (contains? (set (permutations expected)) (batch-partition-by seq partition-fn output-batch-size take-batch-size))
    ;;output-batch-size > input length
    [:a :a :a] identity 5 10 [[:a :a :a]]

    ;;take-batch-size > input length > output-batch-size
    [:a :a :a :a :a] identity 3 10 [[:a :a :a] [:a :a]]

    ;;input length > take-batch-size
    [:a :a :a :a :a :a] identity 3 4 [[:a :a :a] [:a] [:a :a]]

    ;;batch contains multiple groups
    [:a :b :a :b] identity 5 5 [[:a :a] [:b :b]]

    ;;grouped batch gets split
    [:a :b :a :b :a] identity 2 10 [[:a :a] [:a] [:b :b]]

    ;;grouped batches where input length > take-batch-size
    [:a :b :a :b :a :b] identity 2 4 [[:a :a] [:b :b] [:a] [:b]]))

(deftest intersection-with-test
  (are [m1 m2 f expected] (= expected (intersection-with m1 m2 f))
       {:a 1 :b 2 :c 3} {:a 4 :b 5 :c 6} + {:a 5 :b 7 :c 9}
       {:a 1 :b 2} {:b 1 :c 5} vector {:b [2 1]}
       {:a 1 :b 2} {:c 3 :d 4} = {}))

(deftest seq-contains?-test
  (are [col value expected] (= expected (seq-contains? col value))
       [:a :b :c] :a true
       [:a :b :c] :d false))

(deftest implies-test
  (are [p q expected] (= expected (implies p q))
       true true true
       true false false
       false true true
       false false true))

(deftest merge-in-test
  (are [target path ms expected] (= expected (apply merge-in target path ms))
       {} [:a :b] {:c 1} {:a {:b {:c 1}}}
       {:a {:b {:c 1}}} [:a :b] [{:d 2} {:e 3}] {:a {:b {:c 1 :d 2 :e 3}}}))

(deftest validate-email-address-test
  (are [input expected] (= expected (validate-email-address input))
    "foo@bar.com" "foo@bar.com"
    "Commander foo <foo@bar.com>" "foo@bar.com"
    "invalid" false
    :notastring false))
