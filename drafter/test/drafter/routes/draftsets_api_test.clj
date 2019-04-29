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

(deftest delete-draftset-data-for-non-existent-draftset
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [delete-request (tc/with-identity test-manager {:uri "/v1/draftset/missing/data" :request-method :delete :body fs})
          delete-response (route delete-request)]
      (tc/assert-is-not-found-response delete-response))))

(deftest delete-draftset-data-request-with-unknown-content-type
  (with-open [input-stream (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api test-editor)
          delete-request (create-delete-quads-request test-editor draftset-location input-stream "application/unknown-quads-format")
          delete-response (route delete-request)]
      (tc/assert-is-unsupported-media-type-response delete-response))))

(deftest delete-live-graph-not-in-draftset
  (let [quads (statements "test/resources/test-draftset.trig")
        graph-quads (group-by context quads)
        live-graphs (keys graph-quads)
        graph-to-delete (first live-graphs)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (let [{draftset-graphs :changes} (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)]
      (is (= #{graph-to-delete} (set (keys draftset-graphs)))))))

(deftest delete-graph-with-changes-in-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        published-quad (first graph-quads)
        added-quads (rest graph-quads)]
    (publish-quads-through-api [published-quad])
    (append-quads-to-draftset-through-api test-editor draftset-location added-quads)

    (let [{draftset-graphs :changes} (delete-draftset-graph-through-api test-editor draftset-location graph)]
      (is (= #{graph} (set (keys draftset-graphs)))))))

(deftest delete-graph-only-in-draftset
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api test-editor)
        draftset-quads (statements rdf-data-file)
        grouped-quads (group-by context draftset-quads)
        [graph _] (first grouped-quads)]
    (append-data-to-draftset-through-api test-editor draftset-location rdf-data-file)

    (let [{:keys [changes]} (delete-draftset-graph-through-api test-editor draftset-location graph)
          draftset-graphs (keys changes)
          remaining-quads (eval-statements (get-draftset-quads-through-api draftset-location test-editor))
          expected-quads (eval-statements (mapcat second (rest grouped-quads)))
          expected-graphs (keys grouped-quads)]
      (is (= (set expected-quads) (set remaining-quads)))
      (is (= (set expected-graphs) (set draftset-graphs))))))

(deftest delete-graph-request-for-non-existent-draftset
  (let [request (tc/with-identity test-manager {:uri "/v1/draftset/missing/graph" :request-method :delete :params {:graph "http://some-graph"}})
        response (route request)]
    (tc/assert-is-not-found-response response)))

(deftest get-draftset-graph-triples-data
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-data-file "test/resources/test-draftset.trig"
        input-quads (statements draftset-data-file)]
    (append-quads-to-draftset-through-api test-editor draftset-location input-quads)

    (doseq [[graph quads] (group-by context input-quads)]
      (let [graph-triples (set (eval-statements (map map->Triple quads)))
            response-triples (set (get-draftset-graph-triples-through-api draftset-location test-editor graph "false"))]
        (is (= graph-triples response-triples))))))

(deftest get-draftset-quads-data
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-data-file "test/resources/test-draftset.trig"]
    (append-data-to-draftset-through-api test-editor draftset-location draftset-data-file)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location test-editor))
          input-quads (set (eval-statements (statements draftset-data-file)))]
      (is (= input-quads response-quads)))))

(deftest get-draftset-quads-data-with-invalid-accept
  (let [draftset-location (create-draftset-through-api test-editor)]
    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")
    (let [data-request (get-draftset-quads-accept-request draftset-location test-editor "text/invalidrdfformat" "false")
          data-response (route data-request)]
      (tc/assert-is-not-acceptable-response data-response))))

(deftest get-draftset-quads-data-with-multiple-accepted
  (let [draftset-location (create-draftset-through-api test-editor)]
    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")
    (let [accepted "application/n-quads,application/trig,apllication/trix,application/n-triples,application/rdf+xml,text/turtle"
          data-request (get-draftset-quads-accept-request draftset-location test-editor accepted "false")
          data-response (route data-request)]
      (tc/assert-is-ok-response data-response))))

(deftest get-draftset-quads-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (first (keys grouped-quads))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location test-editor "true"))
          expected-quads (set (eval-statements (mapcat second (rest grouped-quads))))]
      (is (= expected-quads response-quads)))))

(deftest get-added-draftset-quads-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph live-quads] (first grouped-quads)
        [draftset-graph draftset-quads] (second grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api live-quads)
    (append-quads-to-draftset-through-api test-editor draftset-location draftset-quads)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location test-editor "true"))
          expected-quads (set (eval-statements (concat live-quads draftset-quads)))]
      (is (= expected-quads response-quads)))))

(deftest get-draftset-triples-for-deleted-graph-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (ffirst grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)

    (let [draftset-triples (get-draftset-graph-triples-through-api draftset-location test-editor graph-to-delete "true")]
      (is (empty? draftset-triples)))))

(deftest get-draftset-triples-for-published-graph-not-in-draftset-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        [graph graph-quads] (first (group-by context quads))
        draftset-location (create-draftset-through-api)]
    (publish-quads-through-api graph-quads)

    (let [draftset-graph-triples (get-draftset-graph-triples-through-api draftset-location test-editor graph "true")
          expected-triples (eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(deftest get-draftset-graph-triples-request-without-graph
  (let [draftset-location (create-draftset-through-api test-editor)]
    (append-quads-to-draftset-through-api test-editor draftset-location (statements "test/resources/test-draftset.trig"))
    (let [data-request {:uri (str draftset-location "/data")
                        :request-method :get
                        :headers {"accept" "application/n-triples"}}
          data-request (tc/with-identity test-editor data-request)
          data-response (route data-request)]
      (tc/assert-is-not-acceptable-response data-response))))


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
