(ns drafter.feature.endpoint.show-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer :all :as t]
   [drafter.feature.draftset.test-helper :as help]
   [drafter.fixture-data :as fd]
   [drafter.test-common :as tc]
   [drafter.util :as util])
  (:import [java.time OffsetDateTime]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(deftest show-public-endpoint-empty-test
  (tc/with-system
    [[:drafter/routes :draftset/api] :drafter/write-scheduler :drafter.fixture-data/loader]
    [system "drafter/feature/empty-db-system.edn"]
    (let [handler (get system [:drafter/routes :draftset/api])]
      (testing "Empty"
        (let [response (help/request-public-endpoint-through-api handler)]
          (tc/assert-is-not-found-response response))))))

(deftest show-public-endpoint-test
  (tc/with-system
    [[:drafter/routes :draftset/api] :drafter/write-scheduler :drafter.fixture-data/loader]
    [system "drafter/feature/empty-db-system.edn"]
    (let [handler (get system [:drafter/routes :draftset/api])
          repo (:drafter/backend system)
          fixtures [(io/resource "drafter/feature/endpoint/show_test-public-endpoint.trig")]]
      (fd/load-fixture! {:repo repo :fixtures fixtures :format :trig})

      (testing "Initialised"
        (let [endpoint (help/get-public-endpoint-through-api handler)
              expected {:id "public"
                        :type "Endpoint"
                        :created-at (OffsetDateTime/parse
                                     "2020-07-03T10:11:46.993Z")
                        :updated-at (OffsetDateTime/parse
                                     "2020-07-06T15:32:52.583Z")
                        :version (util/version
                                  "4a5c8625-4080-471f-a5f0-bddbfce36b51")}]
          (is (= expected endpoint)))))))
