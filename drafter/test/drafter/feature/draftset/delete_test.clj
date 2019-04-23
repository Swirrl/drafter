(ns drafter.feature.draftset.delete-test
  (:require [clojure.test :as t :refer [testing]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-publisher]]
            [grafter-2.rdf.protocols :refer [context]]
            [grafter-2.rdf4j.io :refer [statements]]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(tc/deftest-system-with-keys delete-non-existent-live-graph-in-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        graph-to-delete "http://live-graph"
        delete-request (help/delete-draftset-graph-request test-editor draftset-location "http://live-graph")]

    (testing "silent"
      (let [delete-request (assoc-in delete-request [:params :silent] "true")
            delete-response (handler delete-request)]
        (tc/assert-is-ok-response delete-response)))

    (testing "malformed silent flag"
      (let [delete-request (assoc-in delete-request [:params :silent] "invalid")
            delete-response (handler delete-request)]
        (tc/assert-is-unprocessable-response delete-response)))

    (testing "not silent"
      (let [delete-request (help/delete-draftset-graph-request test-editor draftset-location "http://live-graph")
            delete-response (handler delete-request)]
        (tc/assert-is-unprocessable-response delete-response)))))


(tc/deftest-system-with-keys delete-graph-by-non-owner
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]

  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        [graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location quads)

    (let [delete-request (help/delete-draftset-graph-request test-publisher draftset-location graph)
          delete-response (handler delete-request)]
      (tc/assert-is-forbidden-response delete-response))))
