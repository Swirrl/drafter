(ns drafter.backend.draftset-test
  (:require [clojure.spec.gen.alpha :as g]
            [clojure.test :as t]
            [drafter.backend.draftset :as sut]
            [drafter.test-common :as tc :refer [deftest-system-with-keys]]
            [grafter.rdf :as rdf]
            [grafter.rdf4j.repository :as repo]
            [drafter.fixtures.state-1 :as state]))

(deftest-system-with-keys build-draftset-endpoint-test [:drafter.backend.draftset/endpoint :drafter.fixture-data/loader]
  [{:keys [:drafter.backend.draftset/endpoint] :as sys} "drafter/backend-test.edn"]
  (t/testing "build-draftset-endpoint"
    (t/testing "restricts data access to draftset (union-with-live = true)"
      (with-open [ds-endpoint (-> endpoint
                                  (sut/build-draftset-endpoint "ds-1" true)
                                  repo/->connection)]
        (t/testing "statements"
          (t/is (= state/ds-1-dg-1-data (set (rdf/statements ds-endpoint)))))

        (t/testing "queries"
          (t/testing "select"
            (let [results (set (repo/query ds-endpoint "select distinct ?s where { ?s ?p ?o . }"))
                  tupleify (fn [s] {:s s})
                  expected-resource (->> state/ds-1-subjects
                                         (map tupleify)
                                         set)]
              (t/is (= expected-resource results))))

          ;; todo add construct / ask
          )))))
