(ns drafter.timeouts-test
  (:require [clojure.test :refer :all]
            [drafter.test-common :as tc]
            [drafter.timeouts :refer :all]))

(use-fixtures :each tc/with-spec-instrumentation)

(deftest try-parse-timeout-test
  (testing "non-numeric timeouts invalid"
    (is (instance? Exception (try-parse-timeout "abc"))))

  (testing "negative timeouts invalid"
    (is (instance? Exception (try-parse-timeout "-22"))))

  (testing "should convert valid timeout"
    (is (= 34 (try-parse-timeout "34")))))

(deftest calculate-endpoint-timeout-test
  (testing "With endpoint timeout"
    (let [endpoint-timeout 10
          timeout-fn (calculate-endpoint-timeout endpoint-timeout (constantly 1))
          timeout (timeout-fn {})]
      (is (= endpoint-timeout timeout))))

  (testing "No endpoint timeout"
    (let [default-timeout 5
          timeout-fn (calculate-endpoint-timeout nil (constantly default-timeout))
          timeout (timeout-fn {})]
      (is (= default-timeout timeout)))))

(deftest calculate-unprivileged-timeout-test
  (testing "No request timeout"
    (let [endpoint-timeout 10
          timeout-fn (calculate-unprivileged-timeout (constantly endpoint-timeout))
          request {}
          timeout (timeout-fn request)]
      (is (= endpoint-timeout timeout))))

  (testing "Request timeout > endpoint timeout"
    (let [endpoint-timeout 10
          request-timeout 20
          timeout-fn (calculate-unprivileged-timeout (constantly endpoint-timeout))
          request {:params {:timeout (str request-timeout)}}
          timeout (timeout-fn request)]
      (is (= endpoint-timeout timeout))))

  (testing "Request timeout < endpoint timeout"
    (let [endpoint-timeout 10
          request-timeout 5
          timeout-fn (calculate-unprivileged-timeout (constantly endpoint-timeout))
          request {:params {:timeout (str request-timeout)}}
          timeout (timeout-fn request)]
      (is (= request-timeout timeout)))))

(deftest calculate-privileged-timeout-test
  (let [signing-key "key"
        unprivileged-timeout 20
        timeout-fn (calculate-privileged-timeout signing-key (constantly unprivileged-timeout))]
    (testing "No privileged timeout on request"
      (let [timeout (timeout-fn {})]
        (is (= unprivileged-timeout timeout))))

    (testing "Valid privileged timeout on request"
      (let [privileged-timeout 60
            timeout-param (gen-privileged-timeout privileged-timeout signing-key)
            request {:params {:max-query-timeout timeout-param}}
            timeout (timeout-fn request)]
        (is (= privileged-timeout timeout))))

    (testing "Invalid privileged timeout on request"
      (let [request {:params {:max-query-timeout "invalid timeout"}}
            timeout (timeout-fn request)]
        (is (instance? Exception timeout))))))

(defn- with-unprivileged-timeout [request timeout]
  (assoc-in request [:params :timeout] (str timeout)))

(defn- with-privileged-timeout [request timeout-param]
  (assoc-in request [:params :max-query-timeout] timeout-param))

(deftest calculate-request-timeout-test
  (testing "With no timeouts"
    (let [timeout-fn (calculate-request-query-timeout nil nil)
          timeout (timeout-fn {})]
      (is (= default-query-timeout timeout))))

  (testing "With endpoint timeout"
    (let [endpoint-timeout 10
          timeout-fn (calculate-request-query-timeout endpoint-timeout nil)
          timeout (timeout-fn {})]
      (is (= endpoint-timeout timeout))))

  (testing "Unprivileged timeout < endpoint timeout"
    (let [endpoint-timeout 10
          unprivileged-timeout 5
          timeout-fn (calculate-request-query-timeout endpoint-timeout nil)
          request (with-unprivileged-timeout {} unprivileged-timeout)
          timeout (timeout-fn request)]
      (is (= unprivileged-timeout timeout))))

  (testing "Unprivileged timeout > endpoint timeout"
    (let [endpoint-timeout 10
          unprivileged-timeout 20
          timeout-fn (calculate-request-query-timeout endpoint-timeout nil)
          request (with-unprivileged-timeout {} unprivileged-timeout)
          timeout (timeout-fn request)]
      (is (= endpoint-timeout timeout))))

  (testing "With invalid unprivileged timeout"
    (let [timeout-fn (calculate-request-query-timeout 10 nil)
          request (with-unprivileged-timeout {} "invalid timeout")
          timeout (timeout-fn request)]
      (is (instance? Exception timeout))))

  (testing "Unprivileged timeout < default timeout with no endpoint timeout"
    (let [unprivileged-timeout (- default-query-timeout 10)
          timeout-fn (calculate-request-query-timeout nil nil)
          request (with-unprivileged-timeout {} unprivileged-timeout)
          timeout (timeout-fn request)]
      (is (= unprivileged-timeout timeout))))

  (testing "Unprivileged timeout > default timeout with no endpoint timeout"
    (let [unprivileged-timeout (+ default-query-timeout 10)
          timeout-fn (calculate-request-query-timeout nil nil)
          request (with-unprivileged-timeout {} unprivileged-timeout)
          timeout (timeout-fn request)]
      (is (= default-query-timeout timeout))))

  (testing "With privileged timeout"
    (let [signing-key "key"
          privileged-timeout 20
          endpoint-timeout 10
          timeout-fn (calculate-request-query-timeout endpoint-timeout signing-key)
          privileged-timeout-param (gen-privileged-timeout privileged-timeout signing-key)
          request (with-privileged-timeout {} privileged-timeout-param)
          timeout (timeout-fn request)]
      (is (= privileged-timeout timeout))))

  (testing "With invalid privileged timeout"
    (let [timeout-fn (calculate-request-query-timeout nil "key")
          request (with-unprivileged-timeout {} "invalid")
          timeout (timeout-fn request)]
      (is (instance? Exception timeout)))))
