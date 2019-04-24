(ns drafter.feature.draftset-data.show-test
  (:require [clojure.test :as t]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [drafter.feature.draftset.test-helper :as help]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "test-system.edn")

(tc/deftest-system-with-keys get-draftset-data-for-missing-draftset
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [response (handler (tc/with-identity test-manager {:uri "/v1/draftset/missing/data" :request-method :get :headers {"accept" "application/n-quads"}}))]
    (tc/assert-is-not-found-response response)))

(tc/deftest-system-with-keys get-draftset-data-for-unowned-draftset
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        get-data-request (help/get-draftset-quads-request draftset-location test-publisher :nq "false")
        response (handler get-data-request)]
    (tc/assert-is-forbidden-response response)))
