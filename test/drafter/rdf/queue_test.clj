(ns drafter.rdf.queue-test
  (:require [clojure.test :refer :all]
            [drafter.rdf.queue :refer :all])
  (:import [java.util UUID]))

(deftest queue-tests
  (let [queue (make-queue 2)]

    (testing "offer!"
      (testing "adds a function to the queue"
        (offer! queue identity)
        (is (= 1 (size queue)))))

    (testing "peek-jobs"
      (let [job (first (peek-jobs queue))]

        (is (= identity (:job job)))
        (is (instance? UUID (:id job)))))

    (testing "take!"
      (is (= identity (take! queue)))
      (is (= 0 (size queue))))))
