(ns drafter.responses-test
  (:require [clojure.test :refer :all]
          [drafter.responses :refer :all]
          [drafter.test-common :refer [wrap-db-setup wrap-clean-test-db during-exclusive-write]]
          [drafter.write-scheduler-test :refer [const-job]]))

(deftest submit-async-job-test
  (testing "Submits async job"
    (let [job (const-job :exclusive-write {:result :ok})
          {:keys [status]} (submit-async-job! job)]
      (is (= 202 status))))

  (testing "Returns unavailable response if exclusive write in progress"
    (during-exclusive-write
     (let [job (const-job :batched-write {:result :ok})
           {:keys [status]} (submit-async-job! job)]
       (is (= 503 status))))))

(deftest submit-sync-job-test
  (testing "Invokes response handler with job result"
    (let [job-result {:status :ok :message "success!"}
          job (const-job :sync-write job-result)
          {:keys [status body]} (submit-sync-job! job (fn [r] {:body r :status 200}))]
      (is (= 200 status)
          (= job-result body))))

  (testing "Returns unavailable response if exclusive write in progress"
    (during-exclusive-write
     (let [job (const-job :sync-write {:result :ok})
           {:keys [status]} (submit-sync-job! job)]
       (is (= 503 status))))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
