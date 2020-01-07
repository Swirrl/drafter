(ns ^:rest-api drafter.feature.draftset-data.append-by-graph-test
  (:require [clojure.test :as t :refer :all]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-publisher]]
            [grafter-2.rdf.protocols :refer [context]]
            [grafter-2.rdf4j.io :refer [statements]]
            [martian.encoders :as enc]
            [drafter.async.jobs :as async]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "drafter/feature/empty-db-system.edn")

(defn- copy-live-graph-into-draftset-request
  ([draftset-location user live-graph]
   (copy-live-graph-into-draftset-request draftset-location user live-graph nil))
  ([draftset-location user live-graph metadata]
   (tc/with-identity
     user
     {:uri (str draftset-location "/graph")
      :request-method :put
      :params (cond-> {:graph (str live-graph)}
                      metadata (merge {:metadata metadata}))})))

(defn- copy-live-graph-into-draftset [handler draftset-location user live-graph]
  (let [request (copy-live-graph-into-draftset-request draftset-location user live-graph)
        response (handler request)]
    (tc/await-success (:finished-job (:body response)))))

(tc/deftest-system-with-keys copy-live-graph-into-draftset-test
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (copy-live-graph-into-draftset handler draftset-location test-editor live-graph)

    (let [ds-quads (help/get-draftset-quads-through-api handler draftset-location test-editor "false")
          expected-quads (help/eval-statements quads)]
      (is (= (set expected-quads) (set ds-quads))))))

(tc/deftest-system-with-keys copy-draft-with-metadata-test
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (copy-live-graph-into-draftset handler draftset-location test-editor live-graph)

    (let [request (copy-live-graph-into-draftset-request draftset-location
                                                         test-editor
                                                         live-graph
                                                         (enc/json-encode {:title "Custom job title"}))
          response (handler request)]
      (tc/await-success (:finished-job (:body response)))

      (let [job (-> response :body :finished-job tc/job-path->job-id async/complete-job)]
        (is (= #{:title :draftset :operation} (-> job :metadata keys set)))
        (is (= "Custom job title" (-> job :metadata :title)))))))

(tc/deftest-system-with-keys copy-live-graph-with-existing-draft-into-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        [published private] (split-at 2 quads)
        draftset-location (help/create-draftset-through-api handler test-editor)]

    ;;publish some graph quads to live and some others into the draftset
    (help/publish-quads-through-api handler published)
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location private)

    ;;copy live graph into draftset
    (copy-live-graph-into-draftset handler draftset-location test-editor live-graph)

    ;;draftset graph should contain only the publish quads
    (let [graph-quads (help/get-draftset-quads-through-api handler draftset-location test-editor "false")]
      (is (= (set (help/eval-statements published)) (set graph-quads))))))

(tc/deftest-system-with-keys copy-live-graph-into-unowned-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (let [copy-request (copy-live-graph-into-draftset-request draftset-location test-publisher live-graph)
          copy-response (handler copy-request)]
      (tc/assert-is-forbidden-response copy-response))))

(tc/deftest-system-with-keys copy-non-existent-live-graph
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        copy-request (copy-live-graph-into-draftset-request draftset-location test-editor "http://missing")
        copy-response (handler copy-request)]
    (tc/assert-is-unprocessable-response copy-response)))

(tc/deftest-system-with-keys copy-live-graph-into-non-existent-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (help/publish-quads-through-api handler quads)
    (let [copy-request (copy-live-graph-into-draftset-request "/v1/draftset/missing" test-publisher live-graph)
          copy-response (handler copy-request)]
      (tc/assert-is-not-found-response copy-response))))
