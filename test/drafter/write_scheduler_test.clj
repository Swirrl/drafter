(ns drafter.write-scheduler-test
  (:require [drafter.write-scheduler :refer :all]
            [clojure.test :refer :all])
  (:import [java.util UUID]))

(defn t
  "Just a dummy function"
  [])

(defn mock-job [id type submit-time]
  (->Job id type submit-time t (UUID/randomUUID) (promise)))

(deftest job-sort-order-test
  (let [unordered-jobs [(mock-job 6 :batch-write 2)
                        (mock-job 5 :batch-write 1)
                        (mock-job 4 :exclusive-write 2)
                        (mock-job 3 :exclusive-write 1)
                        (mock-job 2 :sync-write 2)
                        (mock-job 1 :sync-write 1)]

        ordered-jobs (sort compare-jobs unordered-jobs)]

    (is (= [1 2 3 4 5 6] (map :id ordered-jobs)))))
