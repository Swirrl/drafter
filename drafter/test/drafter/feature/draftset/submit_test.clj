(ns ^:rest-api drafter.feature.draftset.submit-test
  (:require [clojure.test :as t :refer [is]]
            [drafter.draftset :as ds]
            [drafter.draftset.spec :as dss]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def keys-for-test [:drafter.fixture-data/loader [:drafter/routes :draftset/api]])

(tc/deftest-system-with-keys submit-non-existent-draftset-to-permission
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [submit-response (handler (help/create-submit-to-permission-request
                                  test-editor
                                  "/v1/draftset/missing"
                                  :drafter:draft:claim))]
    (tc/assert-is-not-found-response submit-response)))

(tc/deftest-system-with-keys submit-draftset-to-permission-by-non-owner
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-response (handler (help/create-submit-to-permission-request
                                  test-publisher
                                  draftset-location
                                  :drafter:draft:claim))]
    (tc/assert-is-forbidden-response submit-response)))

(tc/deftest-system-with-keys submit-draftset-to-user
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        {:keys [body] :as submit-response}
        (-> draftset-location
            (help/submit-draftset-to-user-request test-publisher test-editor)
            (handler))]
    (tc/assert-is-ok-response submit-response)
    (tc/assert-spec ::ds/Draftset body)

    (let [{:keys [current-owner claim-user] :as ds-info} body]
      (is (nil? current-owner))
      (is (= (user/username test-publisher) claim-user)))))

(tc/deftest-system-with-keys submit-draftset-to-user-as-non-owner
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-response (-> draftset-location
                            (help/submit-draftset-to-user-request test-manager test-publisher)
                            (handler))]
    (tc/assert-is-forbidden-response submit-response)))

(tc/deftest-system-with-keys submit-non-existent-draftset-to-user
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [submit-response (-> "/v1/draftset/missing"
                            (help/submit-draftset-to-user-request test-publisher test-editor)
                            (handler))]
    (tc/assert-is-not-found-response submit-response)))

(tc/deftest-system-with-keys submit-draftset-to-non-existent-user
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-response (handler (help/submit-draftset-to-username-request draftset-location "invalid-user@example.com" test-editor))]
    (tc/assert-is-unprocessable-response submit-response)))

(tc/deftest-system-with-keys submit-draftset-without-user-param
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-request (help/submit-draftset-to-user-request draftset-location test-publisher test-editor)
        submit-request (update-in submit-request [:params] dissoc :user)
        response (handler submit-request)]
    (tc/assert-is-unprocessable-response response)))

(tc/deftest-system-with-keys submit-to-with-both-user-and-permission-params
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        request (help/submit-draftset-to-user-request draftset-location test-publisher test-editor)
        request (assoc-in request [:params :permission] "drafter:draft:claim")
        response (handler request)]
    (tc/assert-is-unprocessable-response response)))

(tc/deftest-system-with-keys submit-draftset-to-permission
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-request (help/create-submit-to-permission-request test-editor
                                                                 draftset-location
                                                                 :drafter:draft:claim)
        {ds-info :body :as submit-response} (handler submit-request)]
    (tc/assert-is-ok-response submit-response)
    (tc/assert-spec ::ds/Draftset ds-info)

    (is (= false (contains? ds-info :current-owner)))))

;; The role parameter is deprecated
(tc/deftest-system-with-keys submit-draftset-to-role
  keys-for-test
  [{handler [:drafter/routes :draftset/api]} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        submit-request (help/create-submit-to-role-request test-editor
                                                           draftset-location
                                                           :publisher)
        {ds-info :body :as submit-response} (handler submit-request)]
    (tc/assert-is-ok-response submit-response)
    (tc/assert-spec ::ds/Draftset ds-info)

    (is (= false (contains? ds-info :current-owner)))))
