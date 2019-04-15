(ns drafter-client.client.draftset-test
  (:require [drafter-client.client.draftset :as sut]
            [clojure.test :as t]))

(t/deftest draft-tests
  (t/testing "Draft gets a random id"
    (let [ds-a (sut/->draftset)
          ds-b (sut/->draftset)]
      (t/is (not= (sut/id ds-a) (sut/id ds-b))))))
