(ns drafter.feature.draftset.submit-test
  (:require [drafter.feature.draftset.submit :as sut]
            [clojure.test :as t]))

;; TODO 
#_(defn- submit-draftset-to-username-request [draftset-location target-username user]
  (tc/with-identity user
    {:uri (str draftset-location "/submit-to") :request-method :post :params {:user target-username}}))

#_(defn- submit-draftset-to-user-request [draftset-location target-user user]
  (submit-draftset-to-username-request draftset-location (user/username target-user) user))

#_(defn- submit-draftset-to-user-through-api [draftset-location target-user user]
  (let [request (submit-draftset-to-user-request draftset-location target-user user)
        response (route request)]
    (tc/assert-is-ok-response response)))
