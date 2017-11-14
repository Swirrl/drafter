(ns drafter.write-scheduler-test
  (:require [clojure.test :refer :all]
            [drafter
             [responses :as resp]
             [write-scheduler :refer :all]]
            [drafter.test-helpers.lock-manager :as lm]
            [schema.test :refer [validate-schemas]]
            [swirrl-server.async.jobs :refer [->Job create-job job-succeeded!]]))

(use-fixtures :each validate-schemas)

(defn t
  "Just a dummy function"
  [])

(defn mock-job [id type submit-time]
  (->Job id type submit-time t (promise)))

(defn const-job [priority ret]
  (create-job priority (fn [job] (job-succeeded! job ret))))

(deftest job-sort-order-test
  (let [unordered-jobs [(mock-job 4 :publish-write 2)
                        (mock-job 3 :publish-write 1)
                        (mock-job 2 :background-write 1000) ;; check even later backgrounds sort before earlier publishes
                        (mock-job 1 :background-write 2)
                        (mock-job 0 :background-write 1)]

        ordered-jobs (sort compare-jobs unordered-jobs)]

    (is (= [0 1 2 3 4] (map :id ordered-jobs)))))


(deftest run-sync-job!-test
  (testing "run-sync-job!"
    (testing "when global-writes-lock is unlocked"
      (let [response (resp/run-sync-job! (const-job :blocking-write :done))]
        (is (= 200 (:status response))
            "Job returns 200")
        (is (= :done (get-in response [:body :details :details]))
            "Job executes and returns its value")))

    (testing "when global-writes-lock is locked"
      (let [lock-mgr (lm/build-lock-manager global-writes-lock)]
        (try
          (lm/take-lock! lock-mgr)

          (is (thrown? clojure.lang.ExceptionInfo
                       (resp/run-sync-job! (const-job :blocking-write :done)))
              "Waits a short while for lock, and raises an error when it can't acquire it.")

          (finally
            ;; clean up lock state for next tests
            (lm/release-lock! lock-mgr)))))))

(deftest submit-async-job!-test-1
  (testing "submit-async-job!"
    (testing "when submitting :background-write's"
      (testing "when global-writes-lock is unlocked"
        (let [response (resp/submit-async-job! (const-job :background-write :done))]
          (is (= 202 (:status response))
              "Job returns 202 (Accepted)")
          (is (string? (get-in response [:body :finished-job]))
              "Job executes and returns its value")))

      (testing "when global-writes-lock is locked"
        (let [lock-mgr (lm/build-lock-manager global-writes-lock)]
          (try
            (lm/take-lock! lock-mgr)

            (let [response (resp/submit-async-job! (const-job :background-write :done))]
              ;; :background-write Jobs should still be queued even if
              ;; the writes lock is engaged.  Internal copy operations
              ;; will honor the lock instead.
              (is (= 202 (:status response))
                  "Jobs are accepted in spite of the lock being locked"))

            (finally
              ;; clean up lock state for next tests
              (lm/release-lock! lock-mgr))))))))

(deftest submit-async-job!-test-2
  (testing "when submitting :publish-write's"
    (testing "when global-writes-lock is unlocked"
      (let [response (resp/submit-async-job! (const-job :publish-write :done))]
        (is (= 202 (:status response))
            "Job returns 202 (Accepted)")
        (is (string? (get-in response [:body :finished-job]))
            "Job executes and returns its value")))

    (testing "when global-writes-lock is locked"
      (let [lock-mgr (lm/build-lock-manager global-writes-lock)]
        (try
          (lm/take-lock! lock-mgr)

          (let [response (resp/submit-async-job! (const-job :publish-write :done))]
            ;; :publish-write Jobs should still be queued even if
            ;; the writes lock is engaged.  Internal copy operations
            ;; will honor the lock instead.
            (is (= 202 (:status response))
                "Jobs are accepted in spite of the lock being locked"))

          (finally
            ;; clean up lock state for next tests
            (lm/release-lock! lock-mgr)))))))

;; TODO add tests for batched-write copying etc...

;;(use-fixtures :once wrap-db-setup)
;;(use-fixtures :each wrap-clean-test-db)
