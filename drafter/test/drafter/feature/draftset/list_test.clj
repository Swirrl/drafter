(ns ^:rest-api drafter.feature.draftset.list-test
  (:require
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.test :as t]
   [drafter.draftset :as ds]
   [drafter.feature.draftset.list :as sut]
   [drafter.fixture-data :as fd]
   [drafter.test-common :as tc]
   [drafter.user-test :refer [test-editor test-manager test-publisher]]
   [drafter.util :as dutil])
  (:import java.net.URI
           [java.time OffsetDateTime]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(defn ok-response->specced-body [spec {:keys [body] :as response}]
  (tc/assert-is-ok-response response)
  (tc/assert-spec spec body)
  body)

(defn get-draftsets-request
  [user & {:keys [include union-with-live?]}]
  (let [req {:uri "/v1/draftsets" :request-method :get}
        req (if (some? include) (assoc-in req [:params :include] include) req)
        req (if (some? union-with-live?) (assoc-in req [:params :union-with-live] (str union-with-live?)) req)]
    (tc/with-identity user req)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TESTS
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- assert-visibility [expected-draftset-names {:keys [body] :as response} message]
  (ok-response->specced-body (s/coll-of ::ds/Draftset) response) ;; check we get our draftset json back in an appropriate HTML response

  (let [draftset-names (set (map :display-name body))]
    (t/is (= expected-draftset-names draftset-names)
          message)))

(defn- assert-denies-invalid-login-attempts [handler request]
  (let [invalid-password {"Authorization" (str "Basic " (dutil/str->base64 "editor@swirrl.com:invalidpassword"))}
        invalid-user {"Authorization" (str "Basic " (dutil/str->base64 "nota@user.com:password"))}]

    (t/is (= 401 (:status (handler (assoc request :headers invalid-password)))))
    (t/is (= 401 (:status (handler (assoc request :headers invalid-user)))))))

(tc/deftest-system-with-keys ^:basic-auth get-draftsets-hander-test+all_basic-auth
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/list_test-2.edn"]

  (t/testing "All draftsets"
    (assert-denies-invalid-login-attempts get-draftsets-handler {:uri "/v1/draftsets" :request-method :get :params {:include :all}})))

(tc/deftest-system-with-keys get-draftsets-hander-test+all
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/list_test-2.edn"]

  (t/testing "All draftsets"
    (assert-visibility #{"owned" "publishing"}
                       (get-draftsets-handler (get-draftsets-request test-publisher :include :all))
                       (str"Expected publisher to see owned and publishing draftsets"))

    (assert-visibility #{"editing" "publishing" "admining"}
                       (get-draftsets-handler (get-draftsets-request test-editor :include :all))
                       (str "Expected editor to see editing, publishing & admining draftsets"))

    (assert-visibility #{"publishing" "admining"}
                       (get-draftsets-handler (get-draftsets-request test-manager :include :all))
                       (str "Expected manager to see publishing and admining draftsets"))))

(t/deftest get-draftsets-union-with-live-test
  (tc/with-system
    [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [handler (get system [:drafter/routes :draftset/api])
          repo (:drafter/backend system)
          fixture-resources [(io/resource "drafter/feature/draftset/list_test-union-with-live.trig")]
          request (get-draftsets-request test-editor :union-with-live? true)]
      (fd/load-fixture! {:repo repo :fixtures fixture-resources :format :trig})
      (let [response (handler request)
            draftsets (ok-response->specced-body (s/coll-of ::ds/Draftset) response)
            ds-times (map
                      #(select-keys
                        %
                        [:id :created-at :updated-at :version])
                      draftsets)
            expected [{:id "e06cf827-de24-47ae-8a43-2a1a37c5be4e"
                       :created-at (OffsetDateTime/parse
                                    "2020-07-03T12:12:53.223Z")
                       :updated-at (OffsetDateTime/parse
                                    "2020-07-08T17:40:25.688Z")
                       :version (dutil/merge-versions
                                 (dutil/version
                                 "d2b6a883-cace-4a34-ab75-343f323c12a2")
                                 (dutil/version
                                  "f3871cc6-9cd0-4808-a81b-cb07bc041141"))}
                      {:id "7f94456f-8a92-4d40-8691-2c32f89e9741"
                       :created-at (OffsetDateTime/parse
                                    "2020-07-01T09:55:42.147Z")
                       :updated-at (OffsetDateTime/parse
                                    "2020-07-10T08:04:53.787Z")
                       :version (dutil/merge-versions
                                 (dutil/version
                                  "d1384065-30c8-4a2d-9784-32dfac6d0fa5")
                                 (dutil/version
                                  "f3871cc6-9cd0-4808-a81b-cb07bc041141"))}]]
        (t/is (= (set ds-times) (set expected)))))))

;; NOTE many of these tests are likely dupes of others in this file
(tc/deftest-system-with-keys get-draftsets-handler-test+visibility
  [::sut/get-draftsets-handler :drafter/backend :drafter.fixture-data/loader]
  [{:keys [:drafter/backend]
    get-draftsets-handler ::sut/get-draftsets-handler
    :as sys} "drafter/feature/draftset/list_test-unclaimable.edn"]

  (let [get-draftsets-through-api (fn [include user]
                                    (let [request (get-draftsets-request user :include include)
                                          {:keys [body] :as response} (get-draftsets-handler request)]
                                      (ok-response->specced-body (s/coll-of ::ds/Draftset) response)
                                      body))]

    (t/testing "Missing include filter should return all owned and claimable draftsets"
      (let [request (tc/with-identity test-publisher {:uri "/v1/draftsets" :request-method :get})
            response (get-draftsets-handler request)
            draftsets (ok-response->specced-body (s/coll-of ::ds/Draftset) response)]
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
      (let [request (get-draftsets-request test-publisher :include :invalid)
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
  (let [{claimable-draftsets :body status :status} (get-draftsets-handler (get-draftsets-request test-manager :include :claimable))]
    (t/is (= 200 status))
    (t/is (= 1 (count claimable-draftsets)))))

(tc/deftest-system-with-keys get-draftsets-handler-test+claimable-2
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/list_test-1.edn"]

  ;; This test contains just a single empty draftset created by test-editor.

  (let [{all-draftsets :body} (get-draftsets-handler (get-draftsets-request test-editor :include :all))]
    (t/is (= 1 (count all-draftsets))))

  (let [{all-draftsets :body} (get-draftsets-handler (get-draftsets-request test-publisher :include :all))]
    (t/is (= 0 (count all-draftsets))
          "Other users can't access draftset as it's not been shared (submitted) to them")))

(tc/deftest-system-with-keys get-draftsets-handler-test+changes-claimable
  [:drafter.fixture-data/loader :drafter.feature.draftset.list/get-draftsets-handler]
  [{:keys [drafter.feature.draftset.list/get-draftsets-handler] :as system} "drafter/feature/draftset/claimable-draftset-changes.edn"]
  (let [g1 (URI. "http://graph1")
        g2 (URI. "http://graph2")]

    (let [{ds-infos :body status :status } (get-draftsets-handler (get-draftsets-request test-publisher :include :claimable))
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
        req (get-draftsets-request test-editor :include :owned)
        {ds-infos :body status :status} (get-draftsets-handler req)
        {:keys [changes] :as ds-info} (first ds-infos)]

    (t/is (= 200 status))
    (t/is (= 1 (count ds-infos)))
    (t/is (= :deleted (get-in changes [g1 :status])))
    (t/is (= :created (get-in changes [g2 :status])))))
