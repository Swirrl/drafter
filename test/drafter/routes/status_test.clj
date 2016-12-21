(ns drafter.routes.status-test
  (:require [clojure.test :refer :all]
            [ring.mock.request :refer :all]
            [drafter.common.json-encoders :as enc]
            [drafter.routes.status :refer :all]
            [swirrl-server.async.status-routes :refer [JobNotFinishedResponse]]
            [schema.core :as s]
            [schema.test :refer [validate-schemas]])

  (:import [java.util UUID]
           [java.util.concurrent.locks ReentrantLock]))

(comment use-fixtures :each validate-schemas)

(enc/register-custom-encoders!)

(defonce restart-id (UUID/randomUUID))

(defn lock-responses [lock-value {:keys [status body]}]
  (is (= 200 status))
  (is (= lock-value body)))

(def is-locked (partial lock-responses "true"))

(def is-unlocked (partial lock-responses "false"))

(def no-finished-jobs (atom {}))

(deftest writes-lock-test
  (let [lock (ReentrantLock.)
        status-route (status-routes lock no-finished-jobs restart-id)]

    (testing "GET /writes-locked"
      (testing "when unlocked"
        (is-unlocked (status-route (request :get "/writes-locked")))
        (testing "when locked"
          (.lock lock)
          (is-locked (status-route (request :get "/writes-locked"))))))

    (testing "GET /status"
      (testing "when not found"
        (let [response (status-route (request :get "/finished-jobs/00000000-0000-0000-0000-000000000000"))]
          (is (= restart-id
                 (get-in response [:body :restart-id])))
          (is (s/validate JobNotFinishedResponse
                          response)))))))
