(ns drafter.feature.endpoint.public-test
  (:require
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.test :refer :all]
   [drafter.endpoint :as ep]
   [drafter.feature.draftset.test-helper :as help]
   [drafter.feature.endpoint.public :refer :all]
   [drafter.fixture-data :as fd]
   [drafter.rdf.drafter-ontology :refer :all]
   [drafter.stasher.cache-key :as cache-key]
   [drafter.test-common :as tc]
   [drafter.util :as util]
   [grafter.vocabularies.dcterms :refer :all]
   [grafter.vocabularies.rdf :refer :all])
  (:import [java.time OffsetDateTime]
           [java.time.temporal ChronoUnit]))

(deftest ensure-public-endpoint-empty-test
  (tc/with-system
    [:drafter.stasher/repo [:drafter/routes :draftset/api] :drafter.fixture-data/loader :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          handler (get system [:drafter/routes :draftset/api])]
      (ensure-public-endpoint repo)
      (let [{:keys [created-at updated-at version] :as endpoint}
            (help/get-public-endpoint-through-api handler)]
        (is (tc/equal-up-to (OffsetDateTime/now) created-at 20 ChronoUnit/SECONDS))
        (is (= created-at updated-at))
        (is (s/valid? ::ep/version version))))))

(deftest ensure-public-endpoint-existing-test
  (tc/with-system
    [:drafter.stasher/repo [:drafter/routes :draftset/api] :drafter.fixture-data/loader :drafter/write-scheduler]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          handler (get system [:drafter/routes :draftset/api])
          expected {:id "public"
                    :type "Endpoint"
                    :created-at (OffsetDateTime/parse
                                 "2020-06-18T17:18:06.406Z")
                    :updated-at (OffsetDateTime/parse
                                 "2020-06-19T10:01:45.036Z")
                    :version (util/urn-uuid
                              "ed0b710c-92f1-4a8b-822e-1bf48daeaa6d")}]
      (fd/load-fixture! {:repo     repo
                         :fixtures [(io/resource "drafter/feature/endpoint/public_test-existing.trig")]
                         :format   :trig})
      (ensure-public-endpoint repo)
      (let [endpoint (help/get-public-endpoint-through-api handler)]
        (is (= expected endpoint))))))

(use-fixtures :each tc/with-spec-instrumentation)
