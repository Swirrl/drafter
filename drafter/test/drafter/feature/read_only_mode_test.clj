(ns drafter.feature.read-only-mode-test
  (:require
    [clojure.test :refer :all :as t]
    [drafter.user-test :refer [test-publisher]]
    [drafter.feature.draftset.test-helper :as dh]
    [grafter-2.rdf4j.io :refer [statements]]
    [drafter.test-common :as tc]
    [drafter.async.jobs :refer [->Job create-job job-succeeded!]]
    [drafter.write-scheduler :as scheduler])
  (:import (clojure.lang ExceptionInfo)))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "drafter/feature/empty-db-system.edn")

(def mock-user-id "dummy@user.com")

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

(tc/deftest-system-with-keys reject-mode-test
   [:drafter.fixture-data/loader :drafter/global-writes-lock [:drafter/routes :draftset/api] :drafter/write-scheduler]
   [system system-config]
   (let [global-writes-lock (:drafter/global-writes-lock system)
         handler (get system [:drafter/routes :draftset/api])
         quads (statements "test/resources/test-draftset.trig")
         jobs (take 4 (repeatedly #(job-with-timeout-task :publish-write :done)))
         draftset-location (dh/create-draftset-through-api handler test-publisher)]
     (try
       (doseq [j jobs] (scheduler/queue-job! j))
       ;; execute toggle on another thread
       (tc/timeout 200 #(scheduler/toggle-reject-and-flush!))

       (testing "append requests are rejected when reject (read-only) mode is active"
         (let [append-request (dh/statements->append-request test-publisher draftset-location quads {:format :nq})]
           (is
             (thrown-with-msg?
               ExceptionInfo
               #"Write operations are temporarily unavailable due to maintenance"
               (tc/assert-is-service-unavailable-response (handler append-request))))))

       (testing "submission of async job is rejected when reject (read-only) mode is active"
         (is
           (thrown-with-msg?
             ExceptionInfo
             #"Write operations are temporarily unavailable due to maintenance"
             (tc/assert-is-service-unavailable-response (scheduler/submit-async-job! (nth jobs 0))))))

       (testing "execution of sync job is rejected when reject (read-only) mode is active"
         (is
           (thrown-with-msg?
             ExceptionInfo
             #"Write operations are temporarily unavailable due to maintenance"
             (scheduler/exec-sync-job! global-writes-lock (job-with-timeout-task :blocking-write :done)))))

       ;; wait for first job to definitely complete on processing thread
       @(nth jobs 0)

       (testing "queued jobs are completed before it's reported that jobs are flushed"
         (is (= 1 (count-realised-jobs jobs))
             "only 1 of 4 jobs has been processed after read-only mode activated")

         (is (not (realized? (:jobs-flushed? @scheduler/write-scheduler-admin)))
             "not all queued jobs are complete yet, so :job-flushed? promise has not yet been realised")

         (testing "all queued jobs are now complete and queue is marked as flushed"
           (doseq [j jobs] @j)

           (is (= 4 (count-realised-jobs jobs))
               "all 4 jobs have now been processed")

           (is (realized? (:jobs-flushed? @scheduler/write-scheduler-admin))
               "The jobs queue is now flushed are jobs are complete")))

       (testing "testing again that job submissions are still rejected when reject (read-only) mode is active"
         (let [append-request (dh/statements->append-request test-publisher draftset-location quads {:format :nq})]
           (is
             (thrown-with-msg?
               ExceptionInfo
               #"Write operations are temporarily unavailable due to maintenance"
               (tc/assert-is-service-unavailable-response (handler append-request))))))

       (finally
         (tc/timeout 200 #(scheduler/toggle-reject-and-flush!))))

     (testing "that append requests are accepted after normal write-mode is restored"
       (let [append-request (dh/statements->append-request test-publisher draftset-location quads {:format :nq})
             append-response (handler append-request)]
         (is (= 202 (:status append-response)))
         (is (= :ok (get-in append-response [:body :type])))))

     (testing "that async job submissions are accepted after normal write-mode is restored"
       (let [job (job-with-timeout-task :publish-write :done)
             async-response (scheduler/submit-async-job! job)]
         (is (= 202 (:status async-response)))
         (is (= :ok (get-in async-response [:body :type])))
         @job))))
