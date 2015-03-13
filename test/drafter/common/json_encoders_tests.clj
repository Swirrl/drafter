(ns drafter.common.json-encoders-tests
  (:require [drafter.common.json-encoders :refer :all]
            [clojure.test :refer :all]
            [cheshire.core :refer [generate-string parse-string]]))

(deftest encode-exception-test
  (let [msg "!!!"
        encoded (generate-string (RuntimeException. msg))
        decoded (parse-string encoded)]
    (is (= msg (get decoded "message")))))
