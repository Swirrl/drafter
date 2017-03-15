(ns drafter.write-scheduler-test
  (:require [drafter.write-scheduler :refer :all]
            [clojure.test :refer :all]
            [drafter.test-common :refer [wait-for-lock-ms during-exclusive-write wrap-clean-test-db wrap-db-setup]]
            [swirrl-server.async.jobs :refer [create-job job-succeeded! ->Job]]
            [schema.test :refer [validate-schemas]])

  (:import [java.util UUID]
           [clojure.lang ExceptionInfo]))

(use-fixtures :each validate-schemas)

(defn t
  "Just a dummy function"
  [])

(defn mock-job [id type submit-time]
  (->Job id type submit-time t (promise)))

(defn const-job [priority ret]
  (create-job priority (fn [job] (job-succeeded! job ret))))

(deftest job-sort-order-test
  (let [unordered-jobs [(mock-job 4 :exclusive-write 2)
                        (mock-job 3 :exclusive-write 1)
                        (mock-job 2 :batch-write 2)
                        (mock-job 1 :batch-write 1)]

        ordered-jobs (sort compare-jobs unordered-jobs)]

    (is (= [1 2 3 4] (map :id ordered-jobs)))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
