(ns drafter.feature.draftset.changes-test
  (:require [clojure.test :as t :refer :all]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as ops]
            [drafter.draftset :as ds]
            [drafter.feature.draftset.changes :as sut]
            [drafter.feature.draftset.test-helper :as help :refer [Draftset]]
            [drafter.test-common :as tc]
            [drafter.test-helpers.draft-management-helpers :as mgmth]
            [drafter.user-test :refer [test-editor test-manager test-publisher]]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :refer [context]]
            [grafter-2.rdf4j.io :refer [statements]])
  (:import java.net.URI))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system "drafter/feature/empty-db-system.edn")

(defn- revert-draftset-graph-changes-request [draftset-location user graph]
  (tc/with-identity user {:uri (str draftset-location "/changes") :request-method :delete :params {:graph (str graph)}}))

(defn- revert-draftset-graph-changes-through-api [handler draftset-location user graph]
  (let [{:keys [body] :as response} (handler (revert-draftset-graph-changes-request draftset-location user graph))]
    (tc/assert-is-ok-response response)
    (tc/assert-schema Draftset body)
    body))

(tc/deftest-system-with-keys revert-changes-from-graph-only-in-draftset
  [:drafter.stasher/repo :drafter/write-scheduler]
  [{backend :drafter.stasher/repo} system]
  (let [modified-time (constantly #inst "2017")
        live-graph (URI. "http://live")
        draftset-id (ops/create-draftset! backend test-editor)]
    (mgmt/create-managed-graph! backend live-graph)
    (let [draft-graph (mgmt/create-draft-graph! backend live-graph draftset-id modified-time)
          result (sut/revert-graph-changes! backend draftset-id live-graph)]
      (t/is (= :reverted result))
      (t/is (= false (mgmth/draft-exists? backend draft-graph)))
      (t/is (= false (mgmt/is-graph-managed? backend draft-graph))))))
;; TODO: is this ^^ change right? The `live-graph` returns true, it does seem that
;; it's managed.

(tc/deftest-system-with-keys revert-changes-from-graph-which-exists-in-live
  [:drafter.routes/draftsets-api :drafter.stasher/repo :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api backend :drafter.stasher/repo} system]
  (let [live-graph-uri (tc/make-graph-live! backend (URI. "http://live") (constantly #inst "2017"))
        draftset-id (ops/create-draftset! backend test-editor)
        draft-graph-uri (ops/delete-draftset-graph! backend draftset-id live-graph-uri (constantly #inst "2018"))]
    (let [result (sut/revert-graph-changes! backend draftset-id live-graph-uri)]
      (t/is (= :reverted result))
      (t/is (mgmt/is-graph-managed? backend live-graph-uri))
      (t/is (= false (mgmth/draft-exists? backend draft-graph-uri))))))

(tc/deftest-system-with-keys revert-change-from-graph-which-exists-independently-in-other-draftset
  [:drafter.routes/draftsets-api :drafter.stasher/repo :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api backend :drafter.stasher/repo} system]
  (let [initial-time (constantly #inst "2017")
        live-graph-uri (mgmt/create-managed-graph! backend (URI. "http://live"))
        ds1-id (ops/create-draftset! backend test-editor "ds 1" "description 1" util/create-uuid initial-time)
        ds2-id (ops/create-draftset! backend test-publisher "ds 2" "description 2" util/create-uuid initial-time)

        modified-time (constantly #inst "2018")
        draft-graph1-uri (mgmt/create-draft-graph! backend live-graph-uri ds1-id modified-time)
        draft-graph2-uri (mgmt/create-draft-graph! backend live-graph-uri ds2-id modified-time)]

    (let [result (sut/revert-graph-changes! backend ds2-id live-graph-uri)]
      (t/is (= :reverted result))
      (t/is (mgmt/is-graph-managed? backend live-graph-uri))
      (t/is (= false (mgmth/draft-exists? backend draft-graph2-uri)))
      (t/is (mgmth/draft-exists? backend draft-graph1-uri)))))

(tc/deftest-system-with-keys revert-non-existent-change-in-draftset
  [:drafter.routes/draftsets-api :drafter.stasher/repo :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api backend :drafter.stasher/repo} system]
  (let [draftset-id (ops/create-draftset! backend test-editor)
        result (sut/revert-graph-changes! backend draftset-id (URI. "http://missing"))]
    (t/is (= :not-found result))))

(tc/deftest-system-with-keys revert-changes-in-non-existent-draftset
  [:drafter.routes/draftsets-api :drafter.stasher/repo :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api backend :drafter.stasher/repo} system]
  (let [live-graph (tc/make-graph-live! backend (URI. "http://live") (constantly #inst "2017"))
        result (sut/revert-graph-changes! backend (ds/->DraftsetId "missing") live-graph)]
    (t/is (= :not-found result))))

(tc/deftest-system-with-keys revert-graph-change-in-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
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

(tc/deftest-system-with-keys revert-graph-change-in-unowned-draftset
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (help/delete-draftset-graph-through-api handler test-editor draftset-location live-graph)

    (let [revert-request (revert-draftset-graph-changes-request draftset-location test-publisher live-graph)
          response (handler revert-request)]
      (tc/assert-is-forbidden-response response))))

(tc/deftest-system-with-keys revert-graph-change-in-draftset-unauthorised
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler quads)
    (help/delete-draftset-graph-through-api handler test-editor draftset-location live-graph)

    (let [revert-request {:uri (str draftset-location "/changes") :request-method :delete :params {:graph live-graph}}
          response (handler revert-request)]
      (tc/assert-is-unauthorised-response response))))

(tc/deftest-system-with-keys revert-non-existent-graph-change-in-draftest
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "http://missing")
        response (handler revert-request)]
    (tc/assert-is-not-found-response response)))

(tc/deftest-system-with-keys revert-change-in-non-existent-draftset
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (help/publish-quads-through-api handler quads)
    (let [revert-request (revert-draftset-graph-changes-request "/v1/draftset/missing" test-manager live-graph)
          response (handler revert-request)]
      (tc/assert-is-not-found-response response))))

(tc/deftest-system-with-keys revert-graph-change-request-without-graph-parameter
  [:drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} system]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "tmp")
        revert-request (update-in revert-request [:params] dissoc :graph)
        response (handler revert-request)]
    (tc/assert-is-unprocessable-response response)))
