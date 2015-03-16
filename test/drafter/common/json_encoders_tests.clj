(ns drafter.common.json-encoders-tests
  (:require [drafter.common.json-encoders :refer :all]
            [clojure.test :refer :all]
            [cheshire.core :refer [generate-string parse-string]]))

(register-custom-encoders!)

(defn map-valid-for? [ex-map ex]
  {:pre [(some? ex)]}
  (and (some? ex-map)
       (= (.getMessage ex) (:message ex-map))
       (= (str (class ex)) (:class ex-map))
       (some? (:stack-trace ex-map))
       (if-let [cause (.getCause ex)]
         (map-valid-for? (:cause ex-map) cause)
         (not (contains? ex-map :cause)))))

(deftest exception->map-test
  (testing "no cause"
    (let [ex (RuntimeException. "!!!")
          ex-map (exception->map ex)]
      (is (map-valid-for? ex-map ex))))

  (testing "with cause"
    (let [inner (IllegalStateException. "!!!")
          ex (Exception. ":(" inner)
          ex-map (exception->map ex)]
      (is (map-valid-for? ex-map ex)))))

(deftest encode-exception-test
  (let [msg "!!!"
        encoded (generate-string (RuntimeException. msg))
        decoded (parse-string encoded)]
    (is (= msg (get decoded "message")))
    (is (contains? decoded "stack-trace"))
    (is (contains? decoded "class"))))
