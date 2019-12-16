(ns ^:rest-api drafter.feature.draftset.publish-test
  (:require [clojure.set :as set]
            [clojure.test :as t :refer :all]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]
            [grafter-2.rdf.protocols :refer [context]]
            [grafter-2.rdf4j.io :refer [statements]]
            [drafter.async.jobs :as async]
            [martian.encoders :as enc]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "drafter/feature/empty-db-system.edn")

(tc/deftest-system-with-keys publish-draftset-with-graphs-not-in-live
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        draftset-location (help/create-draftset-through-api handler test-publisher)]
    (help/append-quads-to-draftset-through-api handler test-publisher draftset-location quads)
    (help/publish-draftset-through-api handler draftset-location test-publisher)

    (let [live-quads (help/get-live-quads-through-api handler)]
      (is (= (set (help/eval-statements quads)) (set live-quads))))))

(tc/deftest-system-with-keys
  publish-with-metadata-on-job
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        draftset-location (help/create-draftset-through-api handler test-publisher)]
    (help/append-quads-to-draftset-through-api handler test-publisher draftset-location quads)

    (let [publish-request (help/create-publish-request draftset-location
                                                       test-publisher
                                                       (enc/json-encode {:title "Custom job title"}))
          publish-response (handler publish-request)]
      (tc/await-success (:finished-job (:body publish-response)))

      (let [job (-> publish-response :body :finished-job tc/job-path->job-id async/complete-job)]
        (is (= #{:title :draftset :operation} (-> job :metadata keys set)))
        (is (= "Custom job title" (-> job :metadata :title)))))))

(tc/deftest-system-with-keys publish-draftset-with-statements-added-to-graphs-in-live
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (help/create-draftset-through-api handler test-publisher)
        initial-live-quads (map (comp first second) grouped-quads)
        appended-quads (mapcat (comp rest second) grouped-quads)]

    (help/publish-quads-through-api handler initial-live-quads)
    (help/append-quads-to-draftset-through-api handler test-publisher draftset-location appended-quads)
    (help/publish-draftset-through-api handler draftset-location test-publisher)

    (let [after-publish-quads (help/get-live-quads-through-api handler)]
      (is (= (set (help/eval-statements quads)) (set after-publish-quads))))))

(tc/deftest-system-with-keys publish-draftset-with-statements-deleted-from-graphs-in-live
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (help/create-draftset-through-api handler test-publisher)
        to-delete (map (comp first second) grouped-quads)]
    (help/publish-quads-through-api handler quads)
    (help/delete-quads-through-api handler test-publisher draftset-location to-delete)
    (help/publish-draftset-through-api handler draftset-location test-publisher)

    (let [after-publish-quads (help/get-live-quads-through-api handler)
          expected-quads (help/eval-statements (mapcat (comp rest second) grouped-quads))]
      (is (= (set expected-quads) (set after-publish-quads))))))

(tc/deftest-system-with-keys publish-draftset-with-graph-deleted-from-live
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (help/create-draftset-through-api handler test-publisher)
        graph-to-delete (ffirst grouped-quads)
        expected-quads (help/eval-statements (mapcat second (rest grouped-quads)))]
    (help/publish-quads-through-api handler quads)
    (help/delete-draftset-graph-through-api handler test-publisher draftset-location graph-to-delete)
    (help/publish-draftset-through-api handler draftset-location test-publisher)
    (help/assert-live-quads handler expected-quads)))

(tc/deftest-system-with-keys publish-draftset-with-deletes-and-appends-from-live
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph initial-quads] (first grouped-quads)
        draftset-location (help/create-draftset-through-api handler test-publisher)
        to-add (take 2 (second (second grouped-quads)))
        to-delete (take 1 initial-quads)
        expected-quads (set/difference (set/union (set initial-quads) (set to-add)) (set to-delete))]
    (help/publish-quads-through-api handler initial-quads)
    (help/append-quads-to-draftset-through-api handler test-publisher draftset-location to-add)
    (help/delete-quads-through-api handler test-publisher draftset-location to-delete)
    (help/publish-draftset-through-api handler draftset-location test-publisher)

    (help/assert-live-quads handler expected-quads)))

(tc/deftest-system-with-keys publish-draftest-with-deletions-from-graphs-not-yet-in-live
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [graph graph-quads] (first grouped-quads)
        draftset-location (help/create-draftset-through-api handler test-publisher)]

    ;;delete quads in draftset before they exist in live
    (help/delete-quads-through-api handler test-publisher draftset-location graph-quads)

    ;;add to live then publish draftset
    (help/publish-quads-through-api handler graph-quads)
    (help/publish-draftset-through-api handler draftset-location test-publisher)

    ;;graph should still exist in live
    (help/assert-live-quads handler graph-quads)))

(tc/deftest-system-with-keys publish-non-existent-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} system]
  (let [response (handler (tc/with-identity test-publisher {:uri "/v1/draftset/missing/publish" :request-method :post}))]
    (tc/assert-is-not-found-response response)))

(tc/deftest-system-with-keys publish-by-non-publisher-test
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location (statements "test/resources/test-draftset.trig"))
    (let [publish-response (handler (help/create-publish-request draftset-location test-editor))]
      (tc/assert-is-forbidden-response publish-response))))

(tc/deftest-system-with-keys publish-by-non-owner-test
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-publisher)
        quads (statements "test/resources/test-draftset.trig")]
    (help/append-quads-to-draftset-through-api handler test-publisher draftset-location quads)
    (let [publish-request (help/create-publish-request draftset-location test-manager)
          publish-response (handler publish-request)]
      (tc/assert-is-forbidden-response publish-response))))
