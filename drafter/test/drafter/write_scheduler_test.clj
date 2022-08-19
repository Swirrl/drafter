(ns drafter.write-scheduler-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [drafter.write-scheduler :as scheduler]
            [drafter.write-scheduler :refer [compare-jobs] :as writes]
            [drafter.test-helpers.lock-manager :as lm]
            [schema.test :refer [validate-schemas]]
            [drafter.async.jobs :refer [->Job create-job job-succeeded!]]
            [drafter.test-common :as tc]))

(use-fixtures :each validate-schemas
  tc/with-spec-instrumentation)

(defn t
  "Just a dummy function"
  [])

(def mock-user-id "dummy@user.com")

(defn mock-job [id type submit-time]
  (->Job id mock-user-id nil type submit-time nil nil nil {:operation 'test-job} t (promise)))

(defn const-job [priority ret]
  (create-job mock-user-id
              {:operation 'test-job}
              priority
              (fn [job] (job-succeeded! job ret))))

(deftest job-sort-order-test
  (let [unordered-jobs [(mock-job 4 :publish-write 2)
                        (mock-job 3 :publish-write 1)
                        (mock-job 2 :background-write 1000) ;; check even later backgrounds sort before earlier publishes
                        (mock-job 1 :background-write 2)
                        (mock-job 0 :background-write 1)]

        ordered-jobs (sort compare-jobs unordered-jobs)]

    (is (= [0 1 2 3 4] (map :id ordered-jobs)))))

(def system "drafter/feature/empty-db-system.edn")

(tc/deftest-system-with-keys run-sync-job!-test
  [:drafter/global-writes-lock]
  [{:keys [:drafter/global-writes-lock]} system]
  (let []
    (testing "run-sync-job!"
      (testing "when global-writes-lock is unlocked"
        (let [response (writes/run-sync-job!
                        global-writes-lock (const-job :blocking-write :done))]
          (is (= 200 (:status response))
              "Job returns 200")
          (is (= :done (get-in response [:body :details :details]))
              "Job executes and returns its value")))

     (testing "when global-writes-lock is locked"
       (let [lock-mgr (lm/build-lock-manager (:lock global-writes-lock))]
         (try
           (lm/take-lock! lock-mgr)

           (is (thrown? clojure.lang.ExceptionInfo
                        (writes/run-sync-job!
                         global-writes-lock (const-job :blocking-write :done)))
               "Waits a short while for lock, and raises an error when it can't acquire it.")

           (finally
             ;; clean up lock state for next tests
             (lm/release-lock! lock-mgr))))))))

(tc/deftest-system-with-keys submit-async-job!-test-1
  [:drafter/global-writes-lock]
  [{:keys [:drafter/global-writes-lock]} system]
  (testing "submit-async-job!"
    (testing "when submitting :background-write's"
      (testing "when global-writes-lock is unlocked"
        (let [response (writes/submit-async-job! (const-job :background-write :done))]
          (is (= 202 (:status response))
              "Job returns 202 (Accepted)")
          (is (string? (get-in response [:body :finished-job]))
              "Job executes and returns its value")))

      (testing "when global-writes-lock is locked"
        (let [lock-mgr (lm/build-lock-manager (:lock global-writes-lock))]
          (try
            (lm/take-lock! lock-mgr)

            (let [response (writes/submit-async-job! (const-job :background-write :done))]
              ;; :background-write Jobs should still be queued even if
              ;; the writes lock is engaged.  Internal copy operations
              ;; will honor the lock instead.
              (is (= 202 (:status response))
                  "Jobs are accepted in spite of the lock being locked"))

            (finally
              ;; clean up lock state for next tests
              (lm/release-lock! lock-mgr))))))))

(tc/deftest-system-with-keys submit-async-job!-test-2
  [:drafter/global-writes-lock]
  [{:keys [:drafter/global-writes-lock]} system]
  (testing "when submitting :publish-write's"
    (testing "when global-writes-lock is unlocked"
      (let [response (writes/submit-async-job! (const-job :publish-write :done))]
        (is (= 202 (:status response))
            "Job returns 202 (Accepted)")
        (is (string? (get-in response [:body :finished-job]))
            "Job executes and returns its value")))

    (testing "when global-writes-lock is locked"
      (let [lock-mgr (lm/build-lock-manager (:lock global-writes-lock))]
        (try
          (lm/take-lock! lock-mgr)

          (let [response (writes/submit-async-job! (const-job :publish-write :done))]
            ;; :publish-write Jobs should still be queued even if
            ;; the writes lock is engaged.  Internal copy operations
            ;; will honor the lock instead.
            (is (= 202 (:status response))
                "Jobs are accepted in spite of the lock being locked"))

          (finally
            ;; clean up lock state for next tests
            (lm/release-lock! lock-mgr)))))))

(defn job-with-timeout-task
  "Job which simulates some blocking work being done with a timeout"
  [priority ret]
  (create-job mock-user-id
              {:operation 'test-job}
              priority
              (fn [job]
                (tc/timeout 400 #(Thread/sleep 300))
                (job-succeeded! job ret))))

(defn- count-realised-jobs [jobs]
  (->> (map :value-p jobs)
       (filter realized?)
       (count)))

(tc/deftest-system-with-keys write-loop-with-pauses-test
  [:drafter/global-writes-lock]
  [{:keys [:drafter/global-writes-lock]} system]
  (testing "when submitting :publish-write"
    (let [jobs (take 4 (repeatedly #(job-with-timeout-task :publish-write :done)))
          writer-handles (scheduler/start-writer! global-writes-lock)]
      (try
        (doseq [j jobs] (scheduler/queue-job! j))

        ;; wait for first job to complete on processing thread
        @(nth jobs 0)

        ;; allow a few ticks for second job to start processing on write-loop thread
        (tc/timeout 100 #(Thread/sleep 100))

        ;; toggling writing should pause the write-loop
        (scheduler/toggle-writing!)
        (tc/timeout 50 #(Thread/sleep 10))

        ;; wait for second job to complete on processing thread.
        ;; large timeout is there just in case this test hangs
        (tc/timeout 3000
                    (fn []
                      @(nth jobs 1)))

        (is (= 2 (count-realised-jobs jobs))
            "Only 2 jobs have been processed after write-scheduler paused")

        ;; wait some more, just to see if any more processing occurs (it shouldn't)
        (tc/timeout 1000 #(Thread/sleep 600))

        (is (= 2 (count-realised-jobs jobs))
            "Still only 2 jobs have been processed after write-scheduler paused and we've waited some more")

        ; un-pause / continue writing
        (scheduler/toggle-writing!)

        ;; wait for all jobs tasks in the queue to have finished execution
        (doseq [j jobs] @j)

        (is (= 4 (count-realised-jobs jobs))
            "All 4 jobs have now been processed after write-scheduler un-paused / continued")

        (finally
          (scheduler/stop-writer! writer-handles))))))
