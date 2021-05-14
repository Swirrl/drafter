(ns drafter-client.client.draftset-test
  (:require [drafter-client.client.draftset :as sut]
            [drafter-client.client.util :refer [uuid date-time]]
            [clj-time.core :as time]
            [clojure.test :as t]
            [drafter-client.client.endpoint :as endpoint])
  (:import [java.util UUID]))

(t/deftest draft-tests
  (t/testing "Draft gets a random id"
    (let [ds-a (sut/->draftset)
          ds-b (sut/->draftset)]
      (t/is (not= (sut/id ds-a) (sut/id ds-b))))))

(t/deftest from-json-test
  (let [id (UUID/randomUUID)
        created-at (time/minus (time/now) (time/days 1))
        updated-at (time/plus created-at (time/hours 4))
        version (str "urn:uuid:" (UUID/randomUUID))
        display-name "Test draftset"
        description "Draftset description"
        json {:id (str id)
              :type "Draftset"
              :display-name display-name
              :description description
              :created-at (str created-at)
              :updated-at (str updated-at)
              :version version
              :changes {"http://g1" {:status :updated}
                        "http://g2" {:status :created}}}
        ds (sut/from-json json)]
    (t/is (= id (sut/id ds)))
    (t/is (= id (endpoint/endpoint-id ds)))
    (t/is (= display-name (sut/name ds)))
    (t/is (= description (sut/description ds)))
    (t/is (= created-at (endpoint/created-at ds)))
    (t/is (= updated-at (endpoint/updated-at ds)))
    (t/is (= version (endpoint/version ds)))))
