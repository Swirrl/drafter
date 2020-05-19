(ns ^:rest-api drafter.feature.draftset.show-test
  (:require [clojure.test :as t :refer [is]]
            [grafter-2.rdf.protocols :refer [add context ->Quad ->Triple map->Triple]]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]]
            [drafter.test-common :as tc]
            [drafter.feature.draftset.test-helper :as help]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def keys-for-test [:drafter.fixture-data/loader [:drafter/routes :draftset/api] :drafter/write-scheduler])

(def system-config "drafter/feature/empty-db-system.edn")

(defn- create-submit-to-role-request [user draftset-location role]
  (tc/with-identity user {:uri (str draftset-location "/submit-to") :request-method :post :params {:role (name role)}}))

(defn- submit-draftset-to-role-through-api [handler user draftset-location role]
  (let [response (handler (create-submit-to-role-request user draftset-location role))]
    (tc/assert-is-ok-response response)))

(tc/deftest-system-with-keys get-all-draftsets-changes-test
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        grouped-quads (group-by context (statements "test/resources/test-draftset.trig"))
        [graph1 graph1-quads] (first grouped-quads)
        [graph2 graph2-quads] (second grouped-quads)
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler graph1-quads)

    ;;delete quads from graph1 and insert into graph2
    (help/delete-quads-through-api handler test-editor draftset-location (take 1 graph1-quads))
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location graph2-quads)

    (let [{:keys [changes] :as ds-info} (help/get-draftset-info-through-api handler draftset-location test-editor)]
      (is (= :updated (get-in changes [graph1 :status])))
      (is (= :created (get-in changes [graph2 :status]))))

    ;;delete graph1
    (help/delete-draftset-graph-through-api handler test-editor draftset-location graph1)
    (let [{:keys [changes] :as ds-info} (help/get-draftset-info-through-api handler draftset-location test-editor)]
      (is (= :deleted (get-in changes [graph1 :status])))
      (is (= :created (get-in changes [graph2 :status]))))))

(tc/deftest-system-with-keys get-empty-draftset-without-title-or-description
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-editor)
        ds-info (help/get-draftset-info-through-api handler draftset-location test-editor)]
    (tc/assert-schema help/DraftsetWithoutTitleOrDescription ds-info)))

(tc/deftest-system-with-keys get-empty-draftset-without-description
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        display-name "Test title!"
        draftset-location (help/create-draftset-through-api handler test-editor display-name)
        ds-info (help/get-draftset-info-through-api handler draftset-location test-editor)]
    (tc/assert-schema help/DraftsetWithoutDescription ds-info)
    (is (= display-name (:display-name ds-info)))))

(tc/deftest-system-with-keys get-empty-draftset-with-description
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        display-name "Test title!"
        description "Draftset used in a test"
        draftset-location (help/create-draftset-through-api handler test-editor display-name description)]

    (let [ds-info (help/get-draftset-info-through-api handler draftset-location test-editor)]
      (tc/assert-schema help/draftset-with-description-info-schema ds-info)
      (is (= display-name (:display-name ds-info)))
      (is (= description (:description ds-info))))))

(tc/deftest-system-with-keys get-draftset-containing-data
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        display-name "Test title!"
        draftset-location (help/create-draftset-through-api handler test-editor display-name)
        quads (statements "test/resources/test-draftset.trig")
        live-graphs (set (keys (group-by context quads)))]
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location quads)

    (let [ds-info (help/get-draftset-info-through-api handler draftset-location test-editor)]
      (tc/assert-schema help/DraftsetWithoutDescription ds-info)

      (is (= display-name (:display-name ds-info)))
      (is (= live-graphs (tc/key-set (:changes ds-info)))))))

(tc/deftest-system-with-keys get-draftset-request-for-non-existent-draftset
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        response (handler (help/get-draftset-info-request "/v1/draftset/missing" test-publisher))]
    (tc/assert-is-not-found-response response)))

(tc/deftest-system-with-keys get-draftset-available-for-claim
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (submit-draftset-to-role-through-api handler test-editor draftset-location :publisher)
    (let [ds-info (help/get-draftset-info-through-api handler draftset-location test-publisher)])))

(tc/deftest-system-with-keys get-draftset-for-other-user-test
  keys-for-test
  [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-editor)
        get-request (help/get-draftset-info-request draftset-location test-publisher)
        get-response (handler get-request)]
    (tc/assert-is-forbidden-response get-response)))
