(ns drafter.backend.draftset-test
  (:require [clojure.spec.gen.alpha :as g]
            [clojure.test :as t]
            [drafter.test-common :as tc]
            [drafter.backend.draftset :as sut]
            [drafter.test-common :as tc
             :refer [deftest-system deftest-system-with-keys]]
            [grafter.rdf :as rdf]
            [grafter.rdf4j.repository :as repo]
            [drafter.fixtures.state-1 :as state])
  (:import java.net.URI))

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
          (tc/TODO
           ;; TODO fix this to work with pull datasets.
           ;; Need to implement .evaluate with a single argument.
           ;;
           ;; Note this is isn't used in drafter itself, but would be useful for drafter as an API.
           ;;
           ;; https://github.com/Swirrl/drafter/blob/stasher/src/drafter/backend/draftset/rewrite_result.clj#L89-L107
           (t/testing "select"
             (let [results (set (repo/query ds-endpoint "select distinct ?s where { ?s ?p ?o . }"))
                   tupleify (fn [s] {:s s})
                   expected-resource (->> state/ds-1-subjects
                                          (map tupleify)
                                          set)]
               (t/is (= expected-resource results)))))

          ;; todo add construct / ask
          )))))

(deftest-system draftset-user-restricted-query-test
  [{:keys [:drafter.backend.draftset/endpoint] :as sys} "drafter/backend-test.edn"]
  (t/testing "draftset user restricted query"
    (t/testing "restricts data access to draftset (union-with-live = true)"
      (with-open [ds-endpoint (-> endpoint
                                  (sut/build-draftset-endpoint "ds-1" true)
                                  repo/->connection)]

        (t/testing "Unrestricted (by user) returns allowed graphs"
          (let [q "SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"
                results (set (map :g (repo/query ds-endpoint q)))]
            (t/is (= #{(URI. "http://live-and-ds1-and-ds2")
                       (URI. "http://publishmydata.com/graphs/drafter/draft/ds-1-dg-1")
                       (URI. "http://live-only")}
                     results))))

        (t/testing "query restricted queries restrict draftset"
          (let [q "SELECT ?g FROM NAMED <http://live-only> WHERE { GRAPH ?g { ?s ?p ?o } }"
                results (set (map :g (repo/query ds-endpoint q)))]
            (t/is (= #{(URI. "http://live-only")} results))))

        (t/testing "FROM NAMED outside restriction returns nothing"
          (let [q "SELECT ?g FROM NAMED <http://not-allowed> WHERE { GRAPH ?g { ?s ?p ?o } }"
                results (set (map :g (repo/query ds-endpoint q)))]
            (t/is (= #{} results))))

        (t/testing "FROM NAMED allowed and not-allowed restriction returns only allowed"
          (let [q "SELECT ?g
                   FROM NAMED <http://not-allowed>
                   FROM NAMED <http://live-only>
                   WHERE { GRAPH ?g { ?s ?p ?o } }"
                results (repo/query ds-endpoint q)]
            (t/is (= #{(URI. "http://live-only")}
                     (->> results (map :g) set)))))

        (t/testing "FROM NAMED ignored where :named-graphs specified"
          (let [q "SELECT ?g
                   FROM NAMED <http://not-allowed>
                   FROM NAMED <http://live-only>
                   WHERE { GRAPH ?g { ?s ?p ?o } }"
                results (repo/query ds-endpoint q
                                    :named-graphs ["http://live-and-ds1-and-ds2"])]
            (t/is (= #{(URI. "http://live-and-ds1-and-ds2")}
                     (->> results (map :g) set)))))

        ))))
