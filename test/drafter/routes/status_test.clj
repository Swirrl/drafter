(ns drafter.routes.status-test
  (:require [clojure.test :refer :all]
            [ring.mock.request :refer :all]
            [drafter.common.json-encoders :as enc]
            [drafter.routes.status :refer :all])
  (:import [java.util UUID]
           [java.util.concurrent.locks ReentrantLock]))

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
          (is-locked (status-route (request :get "/writes-locked"))))))))
