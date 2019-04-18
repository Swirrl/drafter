(ns drafter.feature.draftset.submit-test
  (:require [clojure.test :as t :refer [is]]
            [drafter.feature.draftset.test-helper :as help :refer [Draftset]]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(tc/deftest-system-with-keys submit-non-existent-draftset-to-role
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [submit-response (handler (help/create-submit-to-role-request test-editor "/v1/draftset/missing" :publisher))]
    (tc/assert-is-not-found-response submit-response)))

(tc/deftest-system-with-keys submit-draftset-to-role-by-non-owner
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-response (handler (help/create-submit-to-role-request test-publisher draftset-location :manager))]
    (tc/assert-is-forbidden-response submit-response)))

(tc/deftest-system-with-keys submit-draftset-to-invalid-role
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-response (handler (help/create-submit-to-role-request test-editor draftset-location :invalid))]
    (tc/assert-is-unprocessable-response submit-response)))

(tc/deftest-system-with-keys submit-draftset-to-user
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        {:keys [body] :as submit-response}
        (-> draftset-location
            (help/submit-draftset-to-user-request test-publisher test-editor)
            (handler))]
    (tc/assert-is-ok-response submit-response)
    (tc/assert-schema Draftset body)

    (let [{:keys [current-owner claim-user] :as ds-info} body]
      (is (nil? current-owner))
      (is (= (user/username test-publisher) claim-user)))))

(tc/deftest-system-with-keys submit-draftset-to-user-as-non-owner
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-response (-> draftset-location
                            (help/submit-draftset-to-user-request test-manager test-publisher)
                            (handler))]
    (tc/assert-is-forbidden-response submit-response)))

(tc/deftest-system-with-keys submit-non-existent-draftset-to-user
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [submit-response (-> "/v1/draftset/missing"
                            (help/submit-draftset-to-user-request test-publisher test-editor)
                            (handler))]
    (tc/assert-is-not-found-response submit-response)))

(tc/deftest-system-with-keys submit-draftset-to-non-existent-user
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-response (handler (help/submit-draftset-to-username-request draftset-location "invalid-user@example.com" test-editor))]
    (tc/assert-is-unprocessable-response submit-response)))

(tc/deftest-system-with-keys submit-draftset-without-user-param
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-request (help/submit-draftset-to-user-request draftset-location test-publisher test-editor)
        submit-request (update-in submit-request [:params] dissoc :user)
        response (handler submit-request)]
    (tc/assert-is-unprocessable-response response)))

(tc/deftest-system-with-keys submit-to-with-both-user-and-role-params
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        request (help/submit-draftset-to-user-request draftset-location test-publisher test-editor)
        request (assoc-in request [:params :role] "editor")
        response (handler request)]
    (tc/assert-is-unprocessable-response response)))
