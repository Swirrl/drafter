(ns drafter.routes.status-test
  (:require [clojure.test :refer :all]
            [ring.mock.request :refer :all]
            [drafter.common.json-encoders :as enc]
            [drafter.routes.status :refer :all]
            [drafter.write-scheduler :refer [create-job]])
  (:import [java.util UUID]
           [java.util.concurrent.locks ReentrantLock]))

(enc/register-custom-encoders!)
(defonce restart-id (UUID/randomUUID))

(def job-return-value {:type :ok})

(defn create-finished-job []
  (let [job (create-job :batch-write restart-id (fn []))
        p (:value-p job)]
    (deliver p job-return-value)
    job))

(defn create-failed-job [ex]
  (let [job (create-job :batch-write restart-id (fn []))]
    (deliver (:value-p job) {:type :error :exception ex})
    job))

(defn lock-responses [lock-value {:keys [status body]}]
  (is (= 200 status))
  (is (= lock-value body)))

(def is-locked (partial lock-responses "true"))

(def is-unlocked (partial lock-responses "false"))

(def job (create-finished-job))

(def no-finished-jobs (atom {}))

(deftest writes-lock-test
  (let [lock (ReentrantLock.)
        status-route (status-routes lock no-finished-jobs restart-id)]

    (testing "GET /writes-locked"
      (testing "when unlocked"
        (is-unlocked (status-route (request :get "/writes-locked")))
        (testing "when locked"
          (.lock lock)
          (is-locked (status-route (request :get "/writes-locked"))))))))

(def finished-jobs (atom {(:id job) (:value-p job)}))

(defn status-routes-handler [jobs-map]
  (status-routes (ReentrantLock.) (atom jobs-map) restart-id))

(defn finished-job-id-path [id] (str "/finished-jobs/" id))
(def finished-job-path (comp finished-job-id-path :id))

(deftest finished-jobs-test
  (testing "GET /finished-jobs"
    (testing "with a valid finished job"

      (let [job-path (str "/finished-jobs/" (:id job))
            status-route (status-routes (ReentrantLock.)
                                        finished-jobs restart-id)
            {:keys [body status]} (status-route (request :get job-path))]

          (is (= 200 status))
          (is (= {:type :ok
                  :restart-id restart-id} body))))

    (testing "with a failed job"
      (let [msg "job failed"
            {:keys [id value-p] :as job} (create-failed-job (RuntimeException. msg))
            status-route (status-routes-handler {id value-p})
            {:keys [body status]} (status-route (request :get (finished-job-path job)))]
        (is (= 200 status)
            (= msg (get-in body ["exception" "message"])))))

    (testing "with an unknown job"
      (let [job-path (finished-job-id-path (UUID/randomUUID))
            status-route (status-routes-handler {})
            {:keys [status]} (status-route (request :get job-path))]
        (is (= 404 status))))))
