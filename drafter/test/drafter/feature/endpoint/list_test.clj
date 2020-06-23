(ns drafter.feature.endpoint.list-test
  (:require [clojure.test :refer :all]
            [drafter.feature.endpoint.list :refer :all]
            [drafter.endpoint :as ep]
            [drafter.test-common :as tc]
            [drafter.fixture-data :as fd]
            [drafter.user-test :refer [test-publisher]]
            [drafter.feature.draftset.list-test :refer [get-draftsets-request]]
            [clojure.spec.alpha :as s]
            [clojure.java.io :as io])
  (:import [java.time OffsetDateTime]))

(deftest no-login-test
  (tc/with-system
    [:drafter.stasher/repo [:drafter/routes :draftset/api] :drafter.fixture-data/loader :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter.stasher/repo system)
          handler (get system [:drafter/routes :draftset/api])
          expected {:id "public"
                    :type "Endpoint"
                    :created-at (OffsetDateTime/parse "2020-04-01T12:09:57.043Z")
                    :updated-at (OffsetDateTime/parse "2020-04-06T09:18:37.839Z")}]
      (fd/load-fixture! {:repo repo
                         :fixtures [(io/resource "drafter/feature/endpoint/list_test-no-login.trig")]
                         :format :trig})
      (let [req {:uri "/v1/endpoints" :request-method :get}
            {:keys [body] :as resp} (handler req)]
        (tc/assert-is-ok-response resp)
        (tc/assert-spec (s/coll-of ::ep/Endpoint :count 1) body)
        (is (= [expected] body))))))

(defn get-endpoints-request [include user]
  (let [req {:uri "/v1/endpoints" :request-method :get}
        req (if (some? include) (assoc-in req [:params :include] include) req)]
    (tc/with-identity user req)))

(defn- partition-endpoints [endpoints]
  (letfn [(type-filter [t]
            (fn [endpoint] (= t (:type endpoint))))]
    {:public (filter (type-filter "Endpoint") endpoints)
     :draftsets (filter (type-filter "Draftset") endpoints)}))

(deftest with-login-test
  (tc/with-system
    [:drafter.stasher/repo [:drafter/routes :draftset/api] :drafter.fixture-data/loader :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [handler (get system [:drafter/routes :draftset/api])
          expected-public (ep/public (OffsetDateTime/parse "2020-06-10T11:17:46.205Z")
                                     (OffsetDateTime/parse "2020-06-12T08:45:20.902Z"))]
      (fd/load-fixture! {:repo (:drafter.stasher/repo system)
                         :fixtures [(io/resource "drafter/feature/endpoint/list_test-with-login.trig")]
                         :format :trig})

      ;;each test parameterised by the (optional) value of the include parameter
      ;;and whether the result list is expected to contain the public endpoint
      (doseq [[include expect-public?] [[:all true]
                                        [:owned false]
                                        [:claimable false]
                                        [nil true]]]
        (let [endpoints-request (get-endpoints-request include test-publisher)
              draftsets-request (get-draftsets-request include test-publisher)
              {endpoints :body :as endpoints-response} (handler endpoints-request)
              {draftsets :body :as draftsets-response} (handler draftsets-request)
              {public-endpoints :public draftset-endpoints :draftsets} (partition-endpoints endpoints)]
          (tc/assert-is-ok-response endpoints-response)
          (tc/assert-is-ok-response draftsets-response)
          (tc/assert-spec (s/coll-of ::ep/Endpoint :min-count 1) endpoints)

          (if expect-public?
            (is (= [expected-public] public-endpoints))
            (is (= [] public-endpoints)))
          (is (= (set draftsets) (set draftset-endpoints))))))))
