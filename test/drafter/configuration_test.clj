(ns drafter.configuration-test
  (:require [aero.core :as aero]
            [clojure.test :refer :all]
            [drafter.test-common :as tc]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :each validate-schemas
  tc/with-spec-instrumentation)

(defn- read-port [s]
  (aero/reader {} 'port s))

(defn- read-opt-nat [v]
  (aero/reader {} 'opt-nat v))

(deftest read-port-test
  (testing "Valid port"
    (is (= 8080 (read-port "8080"))))

  (testing "Invalid number"
    (is (instance? Exception (read-port "not a port number"))))

  (testing "Port too large"
    (is (instance? Exception (read-port "333333"))))

  (testing "Port non-positive"
    (is (instance? Exception (read-port "0")))))

(deftest opt-nat-test
  (testing "nil"
    (is (nil? (read-opt-nat nil))))

  (testing "Valid string"
    (is (= 5 (read-opt-nat "5"))))

  (testing "Malformed string"
    (is (instance? Exception (read-opt-nat "not a nat"))))

  (testing "Negative string"
    (is (instance? Exception (read-opt-nat "-1"))))

  (testing "Valid number"
    (let [x 4]
      (is (= x (read-opt-nat x)))))

  (testing "Negative number"
    (is (instance? Exception (read-opt-nat -1))))

  (testing "Invalid type"
    (is (instance? Exception (read-opt-nat :not-string-or-number)))))
