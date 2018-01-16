(ns drafter.feature.draftset.claim-test
  (:require [drafter.feature.draftset.claim :as sut]
            [clojure.test :as t]))

;; TODO

#_(defn- create-claim-request [draftset-location user]
  (tc/with-identity user {:uri (str draftset-location "/claim") :request-method :put}))

#_(defn- claim-draftset-through-api [draftset-location user]
  (let [claim-request (create-claim-request draftset-location user)
        {:keys [body] :as claim-response} (route claim-request)]
      (tc/assert-is-ok-response claim-response)
      (tc/assert-schema dset-test/Draftset body)
      body))
