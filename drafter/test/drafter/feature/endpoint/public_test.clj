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
            [clojure.java.io :as io])
  (:import [java.time OffsetDateTime]
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

(use-fixtures :each tc/with-spec-instrumentation)
