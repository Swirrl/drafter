(ns drafter.channels-test
  (:require [clojure.test :refer :all]
            [drafter.channels :refer :all])
  (:import java.util.concurrent.TimeUnit))

(deftest create-send-once-channel-test
  (testing "sends ok"
    (let [[send recv] (create-send-once-channel)]
      (send)
      (let [result (recv 1 TimeUnit/MILLISECONDS)]
        (is (channel-ok? result)))))

  (testing "sends error"
    (let [[send recv] (create-send-once-channel)]
      (send (RuntimeException. ":'("))
      (let [result (recv 1 TimeUnit/MILLISECONDS)]
        (is (channel-error? result)))))

  (testing "Must send throwable"
    (let [[send _] (create-send-once-channel)]
      (is (thrown? IllegalArgumentException (send :not-throwable)))))

  (testing "ok set once"
    (let [[send recv] (create-send-once-channel)]
      (send)
      (send (RuntimeException. "!!!"))
      (let [result (recv 1 TimeUnit/MILLISECONDS)]
        (is (channel-ok? result)))))

  (testing "error set once"
    (let [[send recv] (create-send-once-channel)
          ex1 (RuntimeException. "First error")
          ex2 (RuntimeException. "Second error")]
      (send ex1)
      (send ex2)
      (send)

      (let [result (recv 1 TimeUnit/MILLISECONDS)]
        (is (channel-error? result))
        (is (= ex1 result)))))

  (testing "timeout"
    (let [[_ recv] (create-send-once-channel)
          result (recv 1 TimeUnit/MILLISECONDS)]
      (is (channel-timeout? result)))))
