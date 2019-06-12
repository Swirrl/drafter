(ns ^:rest-api drafter.feature.draftset.set-metadata-test
  (:require [clojure.test :as t :refer [is]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]))

(def system "test-system.edn")

(t/use-fixtures :each tc/with-spec-instrumentation)

(defn- create-update-draftset-metadata-request [user draftset-location title description]
  (tc/with-identity user
    {:uri draftset-location :request-method :put :params {:display-name title :description description}}))

(defn- update-draftset-metadata-through-api [handler user draftset-location title description]
  (let [request (create-update-draftset-metadata-request user draftset-location title description)
        {:keys [body] :as response} (handler request)]
    (tc/assert-is-ok-response response)
    (tc/assert-schema help/Draftset body)
    body))

(tc/deftest-system-with-keys set-draftset-with-existing-title-and-description-metadata
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor "Test draftset" "Test description")
        new-title "Updated title"
        new-description "Updated description"
        {:keys [display-name description]} (update-draftset-metadata-through-api handler test-editor draftset-location new-title new-description)]
    (is (= new-title display-name))
    (is (= new-description description))))

(tc/deftest-system-with-keys set-metadata-for-draftset-with-no-title-or-description
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        new-title "New title"
        new-description "New description"
        {:keys [display-name description]} (update-draftset-metadata-through-api handler test-editor draftset-location new-title new-description)]
    (is (= new-title display-name))
    (is (= new-description description))))

(tc/deftest-system-with-keys set-missing-draftset-metadata
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [meta-request (create-update-draftset-metadata-request test-manager "/v1/draftset/missing" "Title!" "Description")
        meta-response (handler meta-request)]
    (tc/assert-is-not-found-response meta-response)))

(tc/deftest-system-with-keys set-metadata-by-non-owner
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor "Test draftset" "Test description")
        update-request (create-update-draftset-metadata-request test-publisher draftset-location "New title" "New description")
        update-response (handler update-request)]
    (tc/assert-is-forbidden-response update-response)))
