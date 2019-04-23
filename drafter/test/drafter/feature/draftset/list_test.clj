(ns drafter.feature.draftset.list-test
  (:require [clojure.test :as t]
            [drafter.feature.draftset.list :as sut]
            [drafter.routes.draftsets-api-test :as dset-test]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]
            [drafter.util :as dutil]
            [drafter.feature.draftset.test-helper :as help])
  (:import java.net.URI))

(t/use-fixtures :each tc/with-spec-instrumentation)

(defn ok-response->typed-body [schema {:keys [body] :as response}]
  (tc/assert-is-ok-response response)
  (tc/assert-schema schema body)
  body)

(defn get-draftsets-request [include user]
  (tc/with-identity user
    {:uri "/v1/draftsets" :request-method :get :params {:include include}}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TESTS
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- assert-visibility [expected-draftset-names {:keys [body] :as response} message]
  (ok-response->typed-body [help/Draftset] response) ;; check we get our draftset json back in an appropriate HTML response

  (let [draftset-names (set (map :display-name body))]
    (t/is (= expected-draftset-names draftset-names)
          message)))

(defn- assert-denies-invalid-login-attempts [handler request]
  (let [invalid-password {"Authorization" (str "Basic " (dutil/str->base64 "editor@swirrl.com:invalidpassword"))}
        invalid-user {"Authorization" (str "Basic " (dutil/str->base64 "nota@user.com:password"))}]

    (t/is (= 401 (:status (handler (assoc request :headers invalid-password)))))
    (t/is (= 401 (:status (handler (assoc request :headers invalid-user)))))))

(tc/deftest-system-with-keys get-draftsets-hander-test+all
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/list_test-2.edn"]

  (t/testing "All draftsets"
    (assert-denies-invalid-login-attempts get-draftsets-handler {:uri "/v1/draftsets" :request-method :get :params {:include :all}})

    (assert-visibility #{"owned" "publishing"}
                       (get-draftsets-handler (get-draftsets-request :all test-publisher))
                       (str"Expected publisher to see owned and publishing draftsets"))

    (assert-visibility #{"editing" "publishing" "admining"}
                       (get-draftsets-handler (get-draftsets-request :all test-editor))
                       (str "Expected editor to see editing, publishing & admining draftsets"))

    (assert-visibility #{"publishing" "admining"}
                       (get-draftsets-handler (get-draftsets-request :all test-manager))
                       (str "Expected manager to see publishing and admining draftsets"))))

;; NOTE many of these tests are likely dupes of others in this file
(tc/deftest-system-with-keys get-draftsets-handler-test+visibility
  [::sut/get-draftsets-handler :drafter/backend :drafter.fixture-data/loader]
  [{:keys [:drafter/backend]
    get-draftsets-handler ::sut/get-draftsets-handler
    :as sys} "drafter/feature/draftset/list_test-unclaimable.edn"]

  (let [get-draftsets-through-api (fn [include user]
                                    (let [request (get-draftsets-request include user)
                                          {:keys [body] :as response} (get-draftsets-handler request)]

                                      (ok-response->typed-body [help/Draftset] response)

                                      body))]

    (t/testing "Missing include filter should return all owned and claimable draftsets"
      (let [request (tc/with-identity test-publisher {:uri "/v1/draftsets" :request-method :get})
            response (get-draftsets-handler request)
            draftsets (ok-response->typed-body [help/Draftset] response)]
        (t/is (= 2 (count draftsets)))
        (t/is (= #{"owned" "claimable"} (set (map :display-name draftsets))))))

    (t/testing "Owned draftsets"
      (let [draftsets (get-draftsets-through-api :owned test-publisher)]
        (t/is (= 1 (count draftsets)))
        (t/is (= "owned" (:display-name (first draftsets))))))

    (t/testing "Claimable draftsets"
      (let [draftsets (get-draftsets-through-api :claimable test-publisher)]
        (t/is (= 1 (count draftsets)))
        (t/is (= "claimable" (:display-name (first draftsets))))))

    (t/testing "Invalid include parameter"
      (let [request (get-draftsets-request :invalid test-publisher)
            response (get-draftsets-handler request)]
        (tc/assert-is-unprocessable-response response)))

    (t/testing "Unauthenticated"
      (let [response (get-draftsets-handler {:uri "/v1/draftsets" :request-method :get})]
        (tc/assert-is-unauthorised-response response)))))

(tc/deftest-system-with-keys get-draftsets-handler-test+claimable-1
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  ;;a draftset may be in a state where it satisified multiple criteria to be claimed by
  ;;the current user e.g. if a user submits to a role they are in. In this case the user
  ;;can claim it due to being in the claim role, and because they are the submitter of
  ;;an unclaimed draftset. Drafter should only return any matching draftsets once in this
  ;;case

  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/list_test-1-draftset-with-submission.edn"]
  (let [{claimable-draftsets :body status :status} (get-draftsets-handler (get-draftsets-request :claimable test-manager))]
    (t/is (= 200 status))
    (t/is (= 1 (count claimable-draftsets)))))

(tc/deftest-system-with-keys get-draftsets-handler-test+claimable-2
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/list_test-1.edn"]

  ;; This test contains just a single empty draftset created by test-editor.

  (let [{all-draftsets :body} (get-draftsets-handler (get-draftsets-request :all test-editor))]
    (t/is (= 1 (count all-draftsets))))

  (let [{all-draftsets :body} (get-draftsets-handler (get-draftsets-request :all test-publisher))]
    (t/is (= 0 (count all-draftsets))
          "Other users can't access draftset as it's not been shared (submitted) to them")))

(tc/deftest-system-with-keys get-draftsets-handler-test+changes-claimable
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/claimable-draftset-changes.edn"]
  (let [g1 (URI. "http://graph1")
        g2 (URI. "http://graph2")]

    (let [{ds-infos :body status :status } (get-draftsets-handler (get-draftsets-request :claimable test-publisher))
          grouped-draftsets (group-by :display-name ds-infos)
          ds1-info (first (grouped-draftsets "ds1"))
          ds2-info (first (grouped-draftsets "ds2"))]

      (t/is (= 200 status))
      (t/is (= :deleted (get-in ds1-info [:changes g1 :status])))
      (t/is (= :created (get-in ds2-info [:changes g2 :status]))))))

;; NOTE this test is almost identical to the above one, we can probably combine them.
(tc/deftest-system-with-keys get-draftsets-handler-test+changes-owned
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/claimable-draftset-changes-2.edn"]

  (let [g1 (URI. "http://graph1")
        g2 (URI. "http://graph2")
        {ds-infos :body status :status} (get-draftsets-handler (get-draftsets-request :owned test-editor))
        {:keys [changes] :as ds-info} (first ds-infos)]

    (t/is (= 200 status))
    (t/is (= 1 (count ds-infos)))
    (t/is (= :deleted (get-in changes [g1 :status])))
    (t/is (= :created (get-in changes [g2 :status])))))
