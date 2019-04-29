(ns drafter.feature.draftset-data.delete-by-graph-test
  (:require [clojure.test :as t]
            [drafter.test-common :as tc]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :refer :all :as t]
            [drafter.middleware :as middleware]
            [drafter.rdf.drafter-ontology
             :refer
             [drafter:DraftGraph drafter:modifiedAt]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.feature.draftset.create-test :as create-test]
            [drafter.rdf.sparql :as sparql]
            [drafter.routes.draftsets-api :as sut :refer :all]
            [drafter.swagger :as swagger]
            [drafter.test-common :as tc]
            [drafter.timeouts :as timeouts]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [drafter.user.memory-repository :as memrepo]
            [drafter.util :as util]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]]
            [grafter-2.rdf.protocols :refer [add context ->Quad ->Triple map->Triple]]
            [grafter-2.rdf4j.formats :as formats]
            [schema.core :as s]
            [swirrl-server.async.jobs :refer [finished-jobs]]
            [drafter.feature.draftset.test-helper :as help :refer [Draftset]]
            ))

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
