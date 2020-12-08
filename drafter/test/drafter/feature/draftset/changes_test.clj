(ns ^:rest-api drafter.feature.draftset.changes-test
  (:require [clojure.test :as t :refer :all]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as ops]
            [drafter.draftset :as ds]
            [drafter.draftset.spec]
            [drafter.feature.draftset.changes :as sut]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.test-helpers.draft-management-helpers :as mgmth]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :refer [context]]
            [grafter-2.rdf4j.io :refer [statements]]
            [drafter.backend.draftset.graphs :as graphs])
  (:import java.net.URI))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "drafter/feature/empty-db-system.edn")

(defn- revert-draftset-graph-changes-request [draftset-location user graph]
  (tc/with-identity user {:uri (str draftset-location "/changes") :request-method :delete :params {:graph (str graph)}}))

(defn- revert-draftset-graph-changes-through-api [handler draftset-location user graph]
  (let [{:keys [body] :as response} (handler (revert-draftset-graph-changes-request draftset-location user graph))]
    (tc/assert-is-ok-response response)
    (tc/assert-spec ::ds/Draftset body)
    body))

(t/deftest revert-changes-from-graph-only-in-draftset
  (tc/with-system
    [:drafter.stasher/repo ::graphs/manager :drafter/write-scheduler]
    [{backend :drafter.stasher/repo graph-manager ::graphs/manager} system]
    (let [live-graph (URI. "http://live")
          draftset-id (ops/create-draftset! backend test-editor)]
      (let [draft-graph (graphs/create-user-graph-draft graph-manager draftset-id live-graph)
            result (sut/revert-graph-changes! backend draftset-id live-graph)]
        (t/is (= :reverted result))
        (t/is (= false (mgmth/draft-exists? backend draft-graph)))
        (t/is (= false (mgmt/is-graph-managed? backend draft-graph)))))))

(def keys-for-test [[:drafter/routes :draftset/api] :drafter.stasher/repo :drafter/write-scheduler
                    ::graphs/manager])

(tc/deftest-system-with-keys revert-changes-from-graph-which-exists-in-live
  keys-for-test
  [{handler [:drafter/routes :draftset/api] backend :drafter.stasher/repo graph-manager ::graphs/manager} system]
  (let [live-graph-uri (tc/make-graph-live! backend (URI. "http://live"))
        draftset-id (ops/create-draftset! backend test-editor)
        draft-graph-uri (graphs/delete-user-graph graph-manager draftset-id live-graph-uri)]
    (let [result (sut/revert-graph-changes! backend draftset-id live-graph-uri)]
      (t/is (= :reverted result))
      (t/is (mgmt/is-graph-managed? backend live-graph-uri))
      (t/is (= false (mgmth/draft-exists? backend draft-graph-uri))))))

(t/deftest revert-change-from-graph-which-exists-independently-in-other-draftset
  (tc/with-system
    keys-for-test
    [{handler [:drafter/routes :draftset/api] backend :drafter.stasher/repo graph-manager ::graphs/manager} system]
    (let [live-graph-uri (graphs/ensure-managed-user-graph graph-manager (URI. "http://live"))
          ds1-id (ops/create-draftset! backend test-editor "ds 1" "description 1")
          ds2-id (ops/create-draftset! backend test-publisher "ds 2" "description 2")
          draft-graph1-uri (graphs/create-user-graph-draft graph-manager ds1-id live-graph-uri)
          draft-graph2-uri (graphs/create-user-graph-draft graph-manager ds2-id live-graph-uri)]

      (let [result (sut/revert-graph-changes! backend ds2-id live-graph-uri)]
        (t/is (= :reverted result))
        (t/is (mgmt/is-graph-managed? backend live-graph-uri))
        (t/is (= false (mgmth/draft-exists? backend draft-graph2-uri)))
        (t/is (mgmth/draft-exists? backend draft-graph1-uri))))))

(tc/deftest-system-with-keys revert-non-existent-change-in-draftset
  keys-for-test
  [{handler [:drafter/routes :draftset/api] backend :drafter.stasher/repo} system]
  (let [draftset-id (ops/create-draftset! backend test-editor)
        result (sut/revert-graph-changes! backend draftset-id (URI. "http://missing"))]
    (t/is (= :not-found result))))

(tc/deftest-system-with-keys revert-changes-in-non-existent-draftset
  keys-for-test
  [{handler [:drafter/routes :draftset/api] backend :drafter.stasher/repo} system]
  (let [live-graph (tc/make-graph-live! backend (URI. "http://live"))
        result (sut/revert-graph-changes! backend (ds/->DraftsetId "missing") live-graph)]
    (t/is (= :not-found result))))

(tc/deftest-system-with-keys revert-graph-change-in-draftset
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler]
  [{handler [:drafter/routes :draftset/api]} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (help/delete-draftset-graph-through-api handler test-editor draftset-location live-graph)

    (let [{:keys [changes]} (help/get-draftset-info-through-api handler draftset-location test-editor)]
      (is (= #{live-graph} (tc/key-set changes))))

    (let [{:keys [changes] :as ds-info} (revert-draftset-graph-changes-through-api handler draftset-location test-editor live-graph)]
      (is (= #{} (tc/key-set changes))))

    (let [ds-quads (help/get-draftset-quads-through-api handler draftset-location test-editor "true")]

      (is (= (set (help/eval-statements quads)) (set ds-quads))))))

(def keys-for-test-2 [[:drafter/routes :draftset/api] :drafter/write-scheduler])

(tc/deftest-system-with-keys revert-graph-change-in-unowned-draftset
  keys-for-test-2
  [{handler [:drafter/routes :draftset/api]} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (help/delete-draftset-graph-through-api handler test-editor draftset-location live-graph)

    (let [revert-request (revert-draftset-graph-changes-request draftset-location test-publisher live-graph)
          response (handler revert-request)]
      (tc/assert-is-forbidden-response response))))

(def keys-for-test-2 [[:drafter/routes :draftset/api] :drafter/write-scheduler])

(tc/deftest-system-with-keys revert-graph-change-in-draftset-unauthorised
  keys-for-test-2
  [{handler [:drafter/routes :draftset/api]} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (help/delete-draftset-graph-through-api handler test-editor draftset-location live-graph)

    (let [revert-request {:uri (str draftset-location "/changes") :request-method :delete :params {:graph live-graph}}
          response (handler revert-request)]
      (tc/assert-is-unauthorised-response response))))

(tc/deftest-system-with-keys revert-non-existent-graph-change-in-draftest
  keys-for-test-2
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "http://missing")
        response (handler revert-request)]
    (tc/assert-is-not-found-response response)))

(tc/deftest-system-with-keys revert-change-in-non-existent-draftset
  keys-for-test-2
  [{handler [:drafter/routes :draftset/api]} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (help/publish-quads-through-api handler quads)
    (let [revert-request (revert-draftset-graph-changes-request "/v1/draftset/missing" test-manager live-graph)
          response (handler revert-request)]
      (tc/assert-is-not-found-response response))))

(tc/deftest-system-with-keys revert-graph-change-request-without-graph-parameter
  keys-for-test-2
  [{handler [:drafter/routes :draftset/api]} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "tmp")
        revert-request (update-in revert-request [:params] dissoc :graph)
        response (handler revert-request)]
    (tc/assert-is-unprocessable-response response)))

(tc/deftest-system-with-keys draftset-graphs-state-test
  [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler]
  [{handler [:drafter/routes :draftset/api]} system]
  (testing "Graph created"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (help/create-draftset-through-api handler test-editor)]
      (help/append-quads-to-draftset-through-api handler test-editor draftset-location quads)
      (let [{:keys [changes] :as ds-info} (help/get-draftset-info-through-api handler draftset-location test-editor)]
        (is (= :created (get-in changes [live-graph :status]))))))

  (testing "Quads deleted from live graph"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (help/create-draftset-through-api handler test-editor)]
      (help/publish-quads-through-api handler quads)
      (help/delete-quads-through-api handler test-editor draftset-location (take 1 quads))

      (let [{:keys [changes] :as ds-info} (help/get-draftset-info-through-api handler draftset-location test-editor)]
        (is (= :updated (get-in changes [live-graph :status]))))))

  (testing "Quads added to live graph"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          [published to-add] (split-at 1 quads)
          draftset-location (help/create-draftset-through-api handler test-editor)]
      (help/publish-quads-through-api handler published)
      (help/append-quads-to-draftset-through-api handler test-editor draftset-location to-add)

      (let [{:keys [changes] :as ds-info} (help/get-draftset-info-through-api handler draftset-location test-editor)]
        (is (= :updated (get-in changes [live-graph :status]))))))

  (testing "Graph deleted"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (help/create-draftset-through-api handler test-editor)]
      (help/publish-quads-through-api handler quads)
      (help/delete-draftset-graph-through-api handler test-editor draftset-location live-graph)

      (let [{:keys [changes] :as ds-info} (help/get-draftset-info-through-api handler draftset-location test-editor)]
        (is (= :deleted (get-in changes [live-graph :status])))))))
