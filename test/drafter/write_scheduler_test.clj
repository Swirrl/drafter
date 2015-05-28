(ns drafter.write-scheduler-test
  (:require [drafter.write-scheduler :refer :all]
            [clojure.test :refer :all]
            [drafter.test-common :refer [wait-for-lock-ms wrap-with-clean-test-db]]
            [swirrl-server.async.jobs :refer [create-job complete-job! ->Job]])
  (:import [java.util UUID]
           [java.util.concurrent CountDownLatch]
           [clojure.lang ExceptionInfo]))

(defn t
  "Just a dummy function"
  [])

(defn mock-job [id type submit-time]
  (->Job id type submit-time t (promise)))

(defn const-job [priority ret]
  (create-job priority (fn [job] (complete-job! job ret))))

(defn during-exclusive-write-f [f]
  (let [p (promise)
        latch (CountDownLatch. 1)
        exclusive-job (create-job :exclusive-write
                                  (fn [j]
                                    (.countDown latch)
                                    @p))]

    ;; submit exclusive job which should prevent updates from being
    ;; scheduled
    (queue-job exclusive-job)

    ;; wait until exclusive job is actually running i.e. the write lock has
    ;; been taken
    (.await latch)

    (try
      (f)
      (finally
        ;; complete exclusive job
        (deliver p nil)

        ;; wait a short time for the lock to be released
        (wait-for-lock-ms global-writes-lock 200)))))

(defmacro during-exclusive-write [& forms]
  `(during-exclusive-write-f (fn [] ~@forms)))

(deftest job-sort-order-test
  (let [unordered-jobs [(mock-job 6 :batch-write 2)
                        (mock-job 5 :batch-write 1)
                        (mock-job 4 :exclusive-write 2)
                        (mock-job 3 :exclusive-write 1)
                        (mock-job 2 :sync-write 2)
                        (mock-job 1 :sync-write 1)]

        ordered-jobs (sort compare-jobs unordered-jobs)]

    (is (= [1 2 3 4 5 6] (map :id ordered-jobs)))))

(deftest await-sync-job-test
  (testing "Returns job result"
    (let [result {:status :ok}
          job (const-job :sync-write result)]
      (is (= result (await-sync-job! job)))))

  (testing "Throws exception if job cannot be queued"
    (let [sync-job (const-job :sync-write {:status :ok})]
      (during-exclusive-write
       (is (thrown? ExceptionInfo (await-sync-job! sync-job)))))))

(use-fixtures :each wrap-with-clean-test-db)
