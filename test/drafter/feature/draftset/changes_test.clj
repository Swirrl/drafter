(ns drafter.feature.draftset.changes-test
  (:require [clojure.test :as t]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as ops]
            [drafter.feature.draftset.changes :as sut]
            [drafter.test-common :as tc :refer [*test-backend*]]
            [drafter.test-helpers.draft-management-helpers :as mgmth]
            [drafter.user-test :refer [test-editor test-publisher]]
            [drafter.util :as util]
            [drafter.draftset :as ds])
  (:import java.net.URI))

(t/use-fixtures :each
  (tc/wrap-system-setup "test-system.edn" [:drafter.stasher/repo :drafter/write-scheduler])
  tc/with-spec-instrumentation)

(t/deftest revert-changes-from-graph-only-in-draftset
  (let [modified-time (constantly #inst "2017")
        live-graph (URI. "http://live")
        draftset-id (ops/create-draftset! *test-backend* test-editor)]
    (mgmt/create-managed-graph! *test-backend* live-graph)
    (let [draft-graph (mgmt/create-draft-graph! *test-backend* live-graph draftset-id modified-time)
          result (sut/revert-graph-changes! *test-backend* draftset-id live-graph)]
      (t/is (= :reverted result))
      (t/is (= false (mgmth/draft-exists? *test-backend* draft-graph)))
      (t/is (= false (mgmt/is-graph-managed? *test-backend* live-graph))))))

(t/deftest revert-changes-from-graph-which-exists-in-live
  (let [live-graph-uri (tc/make-graph-live! *test-backend* (URI. "http://live") (constantly #inst "2017"))
        draftset-id (ops/create-draftset! *test-backend* test-editor)
        draft-graph-uri (ops/delete-draftset-graph! *test-backend* draftset-id live-graph-uri (constantly #inst "2018"))]
    (let [result (sut/revert-graph-changes! *test-backend* draftset-id live-graph-uri)]
      (t/is (= :reverted result))
      (t/is (mgmt/is-graph-managed? *test-backend* live-graph-uri))
      (t/is (= false (mgmth/draft-exists? *test-backend* draft-graph-uri))))))

(t/deftest revert-change-from-graph-which-exists-independently-in-other-draftset
  (let [initial-time (constantly #inst "2017")
        live-graph-uri (mgmt/create-managed-graph! *test-backend* (URI. "http://live"))
        ds1-id (ops/create-draftset! *test-backend* test-editor "ds 1" "description 1" util/create-uuid initial-time)
        ds2-id (ops/create-draftset! *test-backend* test-publisher "ds 2" "description 2" util/create-uuid initial-time)

        modified-time (constantly #inst "2018")
        draft-graph1-uri (mgmt/create-draft-graph! *test-backend* live-graph-uri ds1-id modified-time)
        draft-graph2-uri (mgmt/create-draft-graph! *test-backend* live-graph-uri ds2-id modified-time)]

    (let [result (sut/revert-graph-changes! *test-backend* ds2-id live-graph-uri)]
      (t/is (= :reverted result))
      (t/is (mgmt/is-graph-managed? *test-backend* live-graph-uri))
      (t/is (= false (mgmth/draft-exists? *test-backend* draft-graph2-uri)))
      (t/is (mgmth/draft-exists? *test-backend* draft-graph1-uri)))))

(t/deftest revert-non-existent-change-in-draftset
  (let [draftset-id (ops/create-draftset! *test-backend* test-editor)
        result (sut/revert-graph-changes! *test-backend* draftset-id (URI. "http://missing"))]
    (t/is (= :not-found result))))

(t/deftest revert-changes-in-non-existent-draftset
  (let [live-graph (tc/make-graph-live! *test-backend* (URI. "http://live") (constantly #inst "2017"))
        result (sut/revert-graph-changes! *test-backend* (ds/->DraftsetId "missing") live-graph)]
    (t/is (= :not-found result))))
