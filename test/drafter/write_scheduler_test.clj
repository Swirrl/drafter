(ns drafter.write-scheduler-test
  (:require [drafter.write-scheduler :refer :all]
            [clojure.test :refer :all]))

(defn t
  "Just a dummy function"
  [])

(defn mock-job [id type submit-time]
  (->Job id type submit-time t (promise)))

(deftest job-sort-order-test
  (let [unordered-jobs [(mock-job 6 :batch 2)
                        (mock-job 5 :batch 1)
                        (mock-job 4 :make-live 2)
                        (mock-job 3 :make-live 1)
                        (mock-job 2 :sync 2)
                        (mock-job 1 :sync 1)]

        ordered-jobs (sort compare-jobs unordered-jobs)]

    (is (= [1 2 3 4 5 6] (map :id ordered-jobs)))))
