(ns drafter.async.jobs-test
  (:require [clojure.test :refer :all]
            [drafter.test-common :as tc]
            [ring.mock.request :refer [request]]
            [clojure.spec.alpha :as s]
            [drafter.async.spec :as spec]
            [drafter.async.jobs :as jobs]
            [drafter.responses :as r]
            [drafter.user-test :refer [test-editor]]
            [cheshire.core :as json]
            [drafter.feature.common :as feat-common])
  (:import clojure.lang.ExceptionInfo
           java.util.UUID))

(defn clear-jobs-fixture [f]
  (reset! jobs/jobs {:pending {} :complete {}})
  (f))

(use-fixtures :each clear-jobs-fixture tc/with-spec-instrumentation)

(def dummy "dummy@user.com")

(deftest create-child-job-test
  (let [parent-fn #(println "parent")
        child-fn #(println "second")
        parent-job (update (jobs/create-job dummy {:operation 'test-job} :batch-write parent-fn)
                           ;; nudge start-time into the past a bit
                           :start-time - 6000)
        child-job (jobs/create-child-job parent-job child-fn)]
    (is (= (::jobs/parent-job-start-time child-job) (:start-time parent-job)))
    (is (= child-fn (:function child-job)) "Failed to update job function")
    (is (> (:start-time child-job) (:start-time parent-job)) "Failed to update job time")
    (is (= (:id parent-job) (:id child-job)) "Job id should not change from parent's")))

(defn- get-job-result [job]
  @(:value-p job))

(defn- assert-failure-result [job expected-message expected-class expected-details]
  (let [{:keys [message error-class details] :as result} (get-job-result job)]
    (is (s/valid? ::spec/failed-job-result result))
    (is (= expected-message message))
    (is (= (.getName expected-class) error-class))
    (is (= expected-details details))))

(deftest job-completed?-test
  (testing "Completed"
    (let [{:keys [value-p] :as job} (jobs/create-job dummy {:operation 'test-job} :batch-write (fn []))]
      (deliver value-p "completed")
      (is (= true (jobs/job-completed? job)))))
  (testing "Not completed"
    (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (fn []))]
      (is (= false (jobs/job-completed? job))))))

(deftest job-failed-test
  (testing "Java exception"
    (let [msg "Failed :("
          ex (IllegalArgumentException. msg)]
      (testing "without details"
        (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (fn []))]
          (jobs/job-failed! job ex)
          (assert-failure-result job msg IllegalArgumentException nil)))

      (testing "with details"
        (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (fn []))
              details {:more :info}]
          (jobs/job-failed! job ex details)
          (assert-failure-result job msg IllegalArgumentException details)))))

  (testing "ExceptionInfo"
    (let [msg "Job failed"
          ex-details {}
          ex (ex-info msg ex-details)]

      (testing "without details"
        (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (fn []))]
          (jobs/job-failed! job ex)
          (assert-failure-result job msg ExceptionInfo ex-details)))

      (testing "with other details"
        (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (fn []))
              details {:other :info}]
          (jobs/job-failed! job ex details)
          (assert-failure-result job msg ExceptionInfo details))))))

(deftest job-succeeded-test
  (testing "With details"
    (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (fn []))
          details {:foo :bar}]
      (jobs/job-succeeded! job details)
      (let [result (get-job-result job)]
        (is (s/valid? ::spec/success-job-result result))
        (is (= details (:details result))))))

  (testing "Without details"
    (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (fn []))]
      (jobs/job-succeeded! job)
      (let [result (get-job-result job)]
        (is (s/valid? ::spec/success-job-result result))
        (is (= false (contains? result :details)))))))

(def job-return-value {:type :ok})

(defn create-finished-job []
  (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (fn []))]
    (jobs/submit-async-job! job)
    (#'jobs/complete-job! job job-return-value)
    job))

(defn create-failed-job [ex]
  (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (fn []))]
    (jobs/submit-async-job! job)
    (#'jobs/complete-job! job {:type :error :exception ex})
    job))

(def job (create-finished-job))

(defn finished-job-id-path [id]
  (str "/v1/status/finished-jobs/" id))

(def finished-job-path (comp finished-job-id-path :id))


(deftest submitted-job-response-test
  (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (fn [j]))
        response (r/submitted-job-response job)]
    (is (s/valid? :submitted-job/response response))))

(deftest batched-job-response-test
  (let [parent-fn #(println "parent")
        child-fn #(println "second")
        parent-job (update (jobs/create-job dummy {:operation 'test-job} :batch-write parent-fn)
                           ;; nudge start-time into the past a bit
                           :start-time - 6000)
        child-job (jobs/create-child-job parent-job child-fn)]
    (is (= (:start-time (jobs/job-response child-job))
           (jobs/timestamp-response (:start-time parent-job))))))


(def system "drafter/feature/empty-db-system.edn")

(defn job=response [job response]
  (= (jobs/job-response job) response))

(defn job=complete-response [job response]
  (= (-> job jobs/job-response (dissoc :status))
     (-> response (assoc :finish-time nil) (dissoc :status))))

(tc/deftest-system-with-keys job-status-test
  [:drafter.routes/jobs-status]
  [{handler :drafter.routes/jobs-status :as sys} system]
  (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (constantly nil))
        path (str "/v1/status/jobs/" (:id job))]

    (jobs/submit-async-job! job)

    (testing "Without auth, 401"
      (let [{:keys [body status] :as response}
            (handler (request :get path))]
        (is (= 401 status))))

    (testing "Submitted job is present, status: :pending"
      (let [{:keys [body status] :as response}
            (handler (tc/with-identity test-editor (request :get path)))]
        (is (= 200 status))
        (is (job=response job body))
        (is (= :pending (:status body)))))

    (testing "Finished job is present, status: :complete"
      (jobs/job-succeeded! job)

      (let [{:keys [body status] :as response}
            (handler (tc/with-identity test-editor (request :get path)))]
        (is (= 200 status))
        (is (job=complete-response job body))
        (is (= :complete (:status body)))))

    (testing "Finished job is present in /finished-jobs/:id"
      (let [{:keys [body status] :as response}
            (handler (tc/with-identity test-editor (request :get (finished-job-path job))))]
        (is (= 200 status))
        (is (= {:type :ok :restart-id r/restart-id} body))))))

(tc/deftest-system-with-keys jobs-list-status-test
  [:drafter.routes/jobs-status :drafter/global-writes-lock]
  [{handler :drafter.routes/jobs-status
    backend :drafter/backend
    global-writes-lock :drafter/global-writes-lock :as sys} system]
  (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (constantly nil))
        path "/v1/status/jobs"]

    (testing "Without auth, 401"
      (let [{:keys [body status] :as response}
            (handler (request :get path))]
        (is (= 401 status))))

    (testing "Submitted job is present, status: :pending"
      (jobs/submit-async-job! job)

      (let [{:keys [body status] :as response}
            (handler (tc/with-identity test-editor (request :get path)))]
        (is (= 200 status))
        (is (job=response job (first body)))
        (is (= :pending (:status (first body))))))

    (testing "Synchronous jobs do not show up in :complete jobs"
      (let [job (feat-common/run-sync {:backend backend
                                       :global-writes-lock global-writes-lock}
                                      dummy 'test-sync-job nil (constantly nil))]
        (let [{:keys [body status] :as response}
              (handler (tc/with-identity test-editor (request :get path)))]
          (is (= 200 status))
          (is (empty? (filter (fn [{:keys [id]}] (= id (:id job))) body))))))))

(tc/deftest-system-with-keys finished-jobs-test
  [:drafter.routes/jobs-status]
  [{handler :drafter.routes/jobs-status :as sys} system]
  (testing "GET /finished-jobs"
    (testing "with a valid finished job"

      (let [job (jobs/create-job dummy {:operation 'test-job} :batch-write (constantly nil))
            _ (jobs/submit-async-job! job)
            _ (jobs/job-succeeded! job)
            job-path (finished-job-path job)
            {:keys [body status]}
            (handler (tc/with-identity test-editor (request :get job-path)))]

        (testing "Without auth, 401"
          (let [{:keys [body status] :as response}
                (handler (request :get job-path))]
            (is (= 401 status))))

        (is (= 200 status))
        (is (= {:type :ok
                :restart-id r/restart-id} body))))

    (testing "with a failed job"
      (let [msg "job failed"
            {:keys [id value-p] :as job} (create-failed-job (RuntimeException. msg))
            {:keys [body status]}
            (handler (tc/with-identity test-editor (request :get (finished-job-path job))))
            body (json/parse-string (json/generate-string body))]
        (is (= 200 status))
        (is (= msg (get-in body ["exception" "message"])))))

    (testing "with an unknown job"
      (let [job-path (finished-job-id-path (UUID/randomUUID))
            status (handler (tc/with-identity test-editor (request :get job-path)))]
        (is (= 404
               (:status status)))
        (is (= r/restart-id
               (get-in status [:body :restart-id])))))

    (testing "with a malformed job id"
      (let [job-path (finished-job-id-path "notaguid")
            {:keys [status body] :as resp}
            (handler (tc/with-identity test-editor (request :get job-path)))]
        (is (= 404 status))
        (is (= r/restart-id (:restart-id body)))))))
