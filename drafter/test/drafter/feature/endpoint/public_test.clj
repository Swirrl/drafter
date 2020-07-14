(ns drafter.feature.endpoint.public-test
  (:require [clojure.test :refer :all]
            [drafter.feature.endpoint.public :refer :all]
            [drafter.test-common :as tc]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter-2.rdf.protocols :refer [->Quad]]
            [grafter.vocabularies.rdf :refer :all]
            [grafter.vocabularies.dcterms :refer :all]
            [drafter.fixture-data :as fd]
            [drafter.endpoint :as ep]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.user-test :refer [test-editor test-system]]
            [clojure.java.io :as io])
  (:import [java.time OffsetDateTime ZoneOffset]
           [java.time.temporal ChronoUnit]))

(deftest ensure-public-endpoint-empty-test
  (tc/with-system
    [:drafter.stasher/repo [:drafter/routes :draftset/api] :drafter.fixture-data/loader :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          handler (get system [:drafter/routes :draftset/api])]
      (ensure-public-endpoint repo)
      (let [{:keys [created-at updated-at] :as endpoint} (help/get-public-endpoint-through-api handler)]
        (is (tc/equal-up-to (OffsetDateTime/now) created-at 20 ChronoUnit/SECONDS))
        (is (= created-at updated-at))))))

(deftest ensure-public-endpoint-existing-test
  (tc/with-system
    [:drafter.stasher/repo [:drafter/routes :draftset/api] :drafter.fixture-data/loader :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          handler (get system [:drafter/routes :draftset/api])
          expected (ep/public (OffsetDateTime/parse "2020-06-18T17:18:06.406Z")
                              (OffsetDateTime/parse "2020-06-19T10:01:45.036Z"))]
      (fd/load-fixture! {:repo     repo
                         :fixtures [(io/resource "drafter/feature/endpoint/public_test-existing.trig")]
                         :format   :trig})
      (ensure-public-endpoint repo)
      (let [endpoint (help/get-public-endpoint-through-api handler)]
        (is (= expected endpoint))))))

(defn within?
  "Whether two datetimes are within the specified period of each other"
  [^OffsetDateTime ref-time ^OffsetDateTime time span units]
  (let [diff (.until ref-time time units)]
    (<= (Math/abs diff) span)))

(deftest create-public-endpoint-test
  (tc/with-system
    [:drafter.stasher/repo [:drafter/routes :draftset/api] :drafter.fixture-data/loader :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [handler (get system [:drafter/routes :draftset/api])
          request (help/create-public-endpoint-request test-system)
          now (OffsetDateTime/now)
          {:keys [body] :as response} (handler request)
          {:keys [created-at updated-at]} body]
      (tc/assert-is-created-response response)
      (tc/assert-spec ::ep/Endpoint body)
      (is (= created-at updated-at))
      (is (within? now created-at 10 ChronoUnit/SECONDS)))))

(deftest create-public-endpoint-created-time-test
  (tc/with-system
    [:drafter.stasher/repo [:drafter/routes :draftset/api] :drafter.fixture-data/loader :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [handler (get system [:drafter/routes :draftset/api])]
      (testing "valid datetime"
        (let [created-at (OffsetDateTime/of 2020 07 12 10 43 56 0 ZoneOffset/UTC)
              request (help/create-public-endpoint-request test-system {:created-at created-at})
              now (OffsetDateTime/now)
              response (handler request)]
          (tc/assert-is-created-response response)
          (let [public-endpoint (help/get-public-endpoint-through-api handler)]
            (is (= created-at (:created-at public-endpoint)))
            (is (within? now (:updated-at public-endpoint) 10 ChronoUnit/SECONDS)))))
      (testing "invalid datetime"
        (let [request (help/create-public-endpoint-request test-system {:created-at "invalid"})
              response (handler request)]
          (tc/assert-is-bad-request-response response))))))

(deftest create-public-endpoint-existing-test
  (tc/with-system
    [:drafter.stasher/repo [:drafter/routes :draftset/api] :drafter.fixture-data/loader :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [handler (get system [:drafter/routes :draftset/api])
          repo (:drafter/backend system)
          fixtures [(io/resource "drafter/feature/endpoint/public_test-existing.trig")]]
      (fd/load-fixture! {:repo repo :fixtures fixtures :format :trig})
      (let [request (help/create-public-endpoint-request test-system)
            {:keys [body] :as response} (handler request)]
        (tc/assert-is-ok-response response)
        (is (= (:created-at body) (OffsetDateTime/parse "2020-06-18T17:18:06.406Z")))
        (is (= (:updated-at body) (OffsetDateTime/parse "2020-06-19T10:01:45.036Z")))))))

(deftest create-public-endpoint-unauthorised-test
  (tc/with-system
    [:drafter.stasher/repo [:drafter/routes :draftset/api] :drafter.fixture-data/loader :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [handler (get system [:drafter/routes :draftset/api])
          request (tc/with-identity test-editor {:request-method :post :uri "/v1/endpoint/public"})
          response (handler request)]
      (tc/assert-is-forbidden-response response))))

(use-fixtures :each tc/with-spec-instrumentation)
