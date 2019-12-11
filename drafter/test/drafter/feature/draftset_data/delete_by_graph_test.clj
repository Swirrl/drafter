(ns ^:rest-api drafter.feature.draftset-data.delete-by-graph-test
  (:require [clojure.test :as t :refer :all]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]
            [grafter-2.rdf.protocols :refer [context]]
            [grafter-2.rdf4j.io :refer [statements]]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "drafter/feature/empty-db-system.edn")

(tc/deftest-system-with-keys delete-live-graph-not-in-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        graph-quads (group-by context quads)
        live-graphs (keys graph-quads)
        graph-to-delete (first live-graphs)
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (let [{draftset-graphs :changes} (help/delete-draftset-graph-through-api handler test-editor draftset-location graph-to-delete)]
      (is (= #{graph-to-delete} (set (keys draftset-graphs)))))))

(tc/deftest-system-with-keys delete-graph-with-changes-in-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        published-quad (first graph-quads)
        added-quads (rest graph-quads)]
    (help/publish-quads-through-api handler [published-quad])
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location added-quads)

    (let [{draftset-graphs :changes} (help/delete-draftset-graph-through-api handler test-editor draftset-location graph)]
      (is (= #{graph} (set (keys draftset-graphs)))))))

(tc/deftest-system-with-keys delete-graph-only-in-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (help/create-draftset-through-api handler test-editor)
        draftset-quads (statements rdf-data-file)
        grouped-quads (group-by context draftset-quads)
        [graph _] (first grouped-quads)]
    (help/append-data-to-draftset-through-api handler test-editor draftset-location rdf-data-file)

    (let [{:keys [changes]} (help/delete-draftset-graph-through-api handler test-editor draftset-location graph)
          draftset-graphs (keys changes)
          remaining-quads (help/eval-statements (help/get-draftset-quads-through-api handler draftset-location test-editor))
          expected-quads (help/eval-statements (mapcat second (rest grouped-quads)))
          expected-graphs (keys grouped-quads)]
      (is (= (set expected-quads) (set remaining-quads)))
      (is (= (set expected-graphs) (set draftset-graphs))))))

(tc/deftest-system-with-keys delete-graph-request-for-non-existent-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [request (tc/with-identity test-manager {:uri "/v1/draftset/missing/graph" :request-method :delete :params {:graph "http://some-graph"}})
        response (handler request)]
    (tc/assert-is-not-found-response response)))

(tc/deftest-system-with-keys delete-graph-by-non-owner
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
 (let [draftset-location (help/create-draftset-through-api handler test-editor)
       [graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location quads)

    (let [delete-request (help/delete-draftset-graph-request test-publisher draftset-location graph)
          delete-response (handler delete-request)]
      (tc/assert-is-forbidden-response delete-response))))

(tc/deftest-system-with-keys delete-live-graph-async-test
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        graph-quads (group-by context quads)
        live-graphs (keys graph-quads)
        graph-to-delete (first live-graphs)
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (let [request (-> (help/delete-draftset-graph-request test-editor
                                                          draftset-location
                                                          graph-to-delete)
                      (assoc-in [:headers "perform-async"] "true"))
          {:keys [status body] :as _job-response} (handler request)
          ds-response (->> {:request-method :get :uri draftset-location}
                           (tc/with-identity test-editor)
                           (handler))]

      ;; did we get a job back?
      (is (= 202 status))
      (is (contains? body :finished-job))
      (is (= :ok (:type body)))

      ;; did the job delete the graph?
      (is (= 200 (:status ds-response)))
      (is (= :deleted (get-in ds-response [:body :changes graph-to-delete :status]))))))
