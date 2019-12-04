(ns drafter.routes.status-test
  (:require [clojure.test :refer :all]
            [drafter.common.json-encoders :as enc]
            [drafter.routes.status :refer :all]
            [drafter.test-common :as tc]
            [ring.mock.request :refer :all]
            [schema.core :as s]
            [schema.test :refer [validate-schemas]]
            [swirrl-server.async.status-routes :refer [JobNotFinishedResponse]])
  (:import java.util.concurrent.locks.ReentrantLock
           java.util.UUID))

(comment use-fixtures :each validate-schemas)

(use-fixtures :each tc/with-spec-instrumentation)

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
        status-route (status-routes {:lock lock})]

    (testing "GET /writes-locked"
      (testing "when unlocked"
        (is-unlocked (status-route (request :get "/writes-locked")))
        (testing "when locked"
          (.lock lock)
          (is-locked (status-route (request :get "/writes-locked"))))))))
