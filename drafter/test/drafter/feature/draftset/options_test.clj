(ns ^:rest-api drafter.feature.draftset.options-test
  (:require [clojure.test :as t :refer [is]]
            [drafter.feature.draftset.create-test :as ct]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user-test
             :refer [test-editor test-manager test-password test-publisher]]
            [drafter.user.memory-repository :as memrepo]))

(tc/deftest-system-with-keys get-options-test
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api]]
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-editor)
        options-request (tc/with-identity test-editor {:uri draftset-location :request-method :options})
        {:keys [body] :as options-response} (handler options-request)]
    (tc/assert-is-ok-response options-response)
    (is (= #{:edit :delete :submit :claim} (set body)))))

(tc/deftest-system-with-keys get-options-for-non-existent-draftset
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api]]
  [system "test-system.edn"]
  (let [handler (get system [:drafter/routes :draftset/api])
        response (handler (tc/with-identity test-manager {:uri "/v1/draftset/missing" :request-method :options}))]
    (tc/assert-is-not-found-response response)))
