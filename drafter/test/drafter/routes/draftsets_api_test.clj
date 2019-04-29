(ns drafter.routes.draftsets-api-test
  (:require [clojure.java.io :as io]
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
            [drafter.feature.draftset.test-helper :as help :refer [Draftset]])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           java.net.URI
           java.time.OffsetDateTime))


(defn- revert-draftset-graph-changes-request [draftset-location user graph]
  (tc/with-identity user {:uri (str draftset-location "/changes") :request-method :delete :params {:graph (str graph)}}))

(defn- revert-draftset-graph-changes-through-api [draftset-location user graph]
  (let [{:keys [body] :as response} (route (revert-draftset-graph-changes-request draftset-location user graph))]
    (tc/assert-is-ok-response response)
    (tc/assert-schema Draftset body)
    body))

(deftest revert-graph-change-in-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location live-graph)

    (let [{:keys [changes]} (help/get-draftset-info-through-api route draftset-location test-editor)]
      (is (= #{live-graph} (tc/key-set changes))))

    (let [{:keys [changes] :as ds-info} (revert-draftset-graph-changes-through-api draftset-location test-editor live-graph)]
      (is (= #{} (tc/key-set changes))))

    (let [ds-quads (get-draftset-quads-through-api draftset-location test-editor "true")]
      (is (= (set (eval-statements quads)) (set ds-quads))))))

(deftest revert-graph-change-in-unowned-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location live-graph)

    (let [revert-request (revert-draftset-graph-changes-request draftset-location test-publisher live-graph)
          response (route revert-request)]
      (tc/assert-is-forbidden-response response))))

(deftest revert-graph-change-in-draftset-unauthorised
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location live-graph)

    (let [revert-request {:uri (str draftset-location "/changes") :request-method :delete :params {:graph live-graph}}
          response (route revert-request)]
      (tc/assert-is-unauthorised-response response))))

(deftest revert-non-existent-graph-change-in-draftest
  (let [draftset-location (create-draftset-through-api test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "http://missing")
        response (route revert-request)]
    (tc/assert-is-not-found-response response)))

(deftest revert-change-in-non-existent-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (publish-quads-through-api quads)
    (let [revert-request (revert-draftset-graph-changes-request "/v1/draftset/missing" test-manager live-graph)
          response (route revert-request)]
      (tc/assert-is-not-found-response response))))

(deftest revert-graph-change-request-without-graph-parameter
  (let [draftset-location (create-draftset-through-api test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "tmp")
        revert-request (update-in revert-request [:params] dissoc :graph)
        response (route revert-request)]
    (tc/assert-is-unprocessable-response response)))

(deftest draftset-graphs-state-test
  (testing "Graph created"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (create-draftset-through-api test-editor)]
      (append-quads-to-draftset-through-api test-editor draftset-location quads)
      (let [{:keys [changes] :as ds-info} (help/get-draftset-info-through-api route draftset-location test-editor)]
        (is (= :created (get-in changes [live-graph :status]))))))

  (testing "Quads deleted from live graph"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (create-draftset-through-api test-editor)]
      (publish-quads-through-api quads)
      (delete-quads-through-api test-editor draftset-location (take 1 quads))

      (let [{:keys [changes] :as ds-info} (help/get-draftset-info-through-api route draftset-location test-editor)]
        (is (= :updated (get-in changes [live-graph :status]))))))

  (testing "Quads added to live graph"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          [published to-add] (split-at 1 quads)
          draftset-location (create-draftset-through-api test-editor)]
      (publish-quads-through-api published)
      (append-quads-to-draftset-through-api test-editor draftset-location to-add)

      (let [{:keys [changes] :as ds-info} (help/get-draftset-info-through-api route draftset-location test-editor)]
        (is (= :updated (get-in changes [live-graph :status]))))))

  (testing "Graph deleted"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (create-draftset-through-api test-editor)]
      (publish-quads-through-api quads)
      (delete-draftset-graph-through-api test-editor draftset-location live-graph)

      (let [{:keys [changes] :as ds-info} (help/get-draftset-info-through-api route draftset-location test-editor)]
        (is (= :deleted (get-in changes [live-graph :status])))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Handler tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ok-response->typed-body [schema {:keys [body] :as response}]
  (tc/assert-is-ok-response response)
  (tc/assert-schema schema body)
  body)
