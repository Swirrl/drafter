(ns drafter.feature.draftset.test-helper
  (:require [drafter.feature.draftset.create-test :as ct]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [drafter.user :as user]))

(defn create-submit-to-role-request [user draftset-location role]
  (tc/with-identity user {:uri (str draftset-location "/submit-to")
                          :request-method :post
                          :params {:role (name role)}}))

(defn create-draftset-through-api [handler user]
  (-> test-editor ct/create-draftset-request handler :headers (get "Location")))

(defn submit-draftset-to-username-request [draftset-location target-username user]
  (tc/with-identity user {:uri (str draftset-location "/submit-to")
                          :request-method :post
                          :params {:user target-username}}))

(defn submit-draftset-to-user-request [draftset-location target-user user]
  (submit-draftset-to-username-request draftset-location (user/username target-user) user))

(defn submit-draftset-to-user-through-api [handler draftset-location target-user user]
  (let [request (submit-draftset-to-user-request draftset-location target-user user)
        response (handler request)]
    (tc/assert-is-ok-response response)))
