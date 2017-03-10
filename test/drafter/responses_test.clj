(ns drafter.responses-test
  (:require [clojure.test :refer :all]
            [drafter.responses :refer :all]
            [swirrl-server.errors :refer [encode-error]]
            [drafter.test-common :refer [throws-exception? wrap-db-setup wrap-clean-test-db during-exclusive-write]]
            [drafter.write-scheduler-test :refer [const-job]]))

(deftest submit-async-job-test
  (testing "Submits async job"
    (let [job (const-job :exclusive-write {:result :ok})
          {:keys [status]} (submit-async-job! job)]
      (is (= 202 status))))

  (testing "Returns unavailable response if exclusive write in progress"
    (during-exclusive-write
     (let [job (const-job :batched-write {:result :ok})]
       (throws-exception?
        (submit-async-job! job)
        (catch clojure.lang.ExceptionInfo ex
          (= 503 (:status (encode-error ex)))))))))

(deftest run-sync-job-test
  (testing "Invokes response handler with job result"
    (let [job-result {:status :ok :message "success!"}
          job (const-job :sync-write job-result)
          {:keys [status body]} (run-sync-job! job (fn [r] {:body r :status 200}))]
      (is (= 200 status)
          (= job-result body))))

  (testing "Returns unavailable response if exclusive write in progress"
    (let [job (const-job :sync-write {:result :ok})]
      (during-exclusive-write
       (throws-exception?
        (run-sync-job! job)
        (catch clojure.lang.ExceptionInfo ex
          (= 503 (:status (encode-error ex)))))))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
