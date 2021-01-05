(ns drafter.backend.draftset-test
  (:require [clojure.test :as t]
            [drafter.backend.draftset :as sut]
            [drafter.test-common :as tc :refer [deftest-system-with-keys with-system]]
            [grafter-2.rdf4j.io :as rio]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf.protocols :as pr]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [drafter.fixture-data :as fd])
  (:import [java.net URI]))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def live-drafted-live #{(pr/->Quad (URI. "http://s1") (URI. "http://p1") (URI. "http://o1") (URI. "http://live-drafted"))})
(def live-drafted-ds1 #{(pr/->Quad (URI. "http://s2") (URI. "http://p2") (URI. "http://o2") (URI. "http://live-drafted"))
                        (pr/->Quad (URI. "http://s3") (URI. "http://p3") (URI. "http://unpublished-graph-ds1") (URI. "http://live-drafted"))
                        (pr/->Quad (URI. "http://ds1") (URI. "http://dp1") (URI. "http://do1") (URI. "http://live-drafted"))})
(def unpublished-ds1 #{(pr/->Quad (URI. "http://unpublished-s1") (URI. "http://unpublished-p1") (URI. "http://unpublished-o1") (URI. "http://unpublished-graph-ds1"))})
(def live-only #{(pr/->Quad (URI. "http://live-only-s") (URI. "http://live-only-p") (URI. "http://live-only-o") (URI. "http://live-only"))})

(defn- statement->spog [{:keys [s p o c] :as stmt}]
  {:s s :p p :o o :g c})

(t/deftest build-draftset-endpoint-test
  (with-system
    [:drafter/backend :drafter.fixture-data/loader]
    [sys "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend sys)]
      (fd/load-fixture! {:repo repo :format :trig :fixtures [(io/resource "drafter/backend/draftset_test/drafter-state.trig")]})
      (t/testing "build-draftset-endpoint"
        (t/testing "draftset (union-with-live = true)"
          (let [draft-endpoint (sut/build-draftset-endpoint repo "ds-1" true)]
            (with-open [conn (repo/->connection draft-endpoint)]
              (t/testing "statements"
                (let [expected (set/union live-drafted-ds1 unpublished-ds1 live-only)
                      expected-triples (set (map pr/map->Triple expected))
                      actual (set (rio/statements conn :grafter.repository/infer false))]
                  (t/is (= expected-triples actual))))

              (t/testing "queries"
                (t/testing "SELECT"
                  (let [results (set (repo/query conn "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }"))
                        expected-statements (set/union live-drafted-ds1 unpublished-ds1 live-only)
                        expected-bindings (set (map statement->spog expected-statements))]
                    (t/is (= expected-bindings results))))

                (t/testing "applies connection dataset"
                  (let [q "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }"
                        results (set (repo/query conn q :named-graphs ["http://unpublished-graph-ds1"]))
                        expected-bindings (set (map statement->spog unpublished-ds1))]
                    (t/is (= expected-bindings results))))

                (t/testing "applies query dataset"
                  (let [results (set (repo/query conn "SELECT * FROM NAMED <http://live-only> WHERE { GRAPH ?g { ?s ?p ?o } }"))
                        expected-bindings (set (map statement->spog live-only))]
                    (t/is (= expected-bindings results))))

                (t/testing "connection dataset should override query dataset"
                  (let [q "SELECT * FROM NAMED <http://live-drafted> WHERE { GRAPH ?g { ?s ?p ?o } }"
                        results (set (repo/query conn q :named-graphs ["http://unpublished-graph-ds1"]))
                        expected-bindings (set (map statement->spog unpublished-ds1))]
                    (t/is (= expected-bindings results))))

                (t/testing "CONSTRUCT"
                  (let [q "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH ?g { ?s ?p ?o } }"
                        results (set (repo/query conn q))
                        expected (set/union live-drafted-ds1 unpublished-ds1 live-only)
                        expected-triples (set (map pr/map->Triple expected))]
                    (t/is (= expected-triples results))))

                (t/testing "ASK"
                  (let [result (repo/query conn "ASK WHERE { GRAPH <http://live-only> { ?s ?p ?o } }")]
                    (t/is (= true result))))))))

        (t/testing "union-with-live = false"
          (let [draft-endpoint (sut/build-draftset-endpoint repo "ds-1" false)]
            (with-open [conn (repo/->connection draft-endpoint)]
              (t/testing "statements"
                (let [expected (set/union live-drafted-ds1 unpublished-ds1)
                      expected-triples (set (map pr/map->Triple expected))
                      actual (set (rio/statements conn :grafter.repository/infer false))]
                  (t/is (= expected-triples actual))))

              (t/testing "SELECT"
                (let [results (set (repo/query conn "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }"))
                      expected-statements (set/union live-drafted-ds1 unpublished-ds1)
                      expected-bindings (set (map statement->spog expected-statements))]
                  (t/is (= expected-bindings results))))

              (t/testing "CONSTRUCT"
                (let [results (set (repo/query conn "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH ?g { ?s ?p ?o } }"))
                      expected (set/union live-drafted-ds1 unpublished-ds1)
                      expected-triples (set (map pr/map->Triple expected))]
                  (t/is (= expected-triples results))))

              (t/testing "ASK"
                (let [result (repo/query conn "ASK WHERE { GRAPH <http://live-only> { ?s ?p ?o } }")]
                  (t/is (= false result))))

              (t/testing "applies connection dataset"
                (let [results (set (repo/query conn "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }" :named-graphs ["http://live-drafted"]))
                      expected-bindings (set (map statement->spog live-drafted-ds1))]
                  (t/is (= expected-bindings results))))

              (t/testing "applies query dataset"
                (let [results (set (repo/query conn "SELECT * FROM NAMED <http://unpublished-graph-ds1> WHERE { GRAPH ?g { ?s ?p ?o } }"))
                      expected-triples (set (map statement->spog unpublished-ds1))]
                  (t/is (= expected-triples results))))

              (t/testing "excludes query graphs not draft"
                (let [results (set (repo/query conn "SELECT * FROM NAMED <http://live-only> WHERE { GRAPH ?g { ?s ?p ?o } }"))]
                  ;;no connection dataset so the query dataset should be used
                  ;;<http://live-only> graph is not visible in the draft when union-with-live is false so result set
                  ;;should be empty
                  (t/is (= #{} results))))

              (t/testing "connection dataset should override query dataset"
                (let [q "SELECT * FROM NAMED <http://live-drafted> WHERE { GRAPH ?g { ?s ?p ?o } }"
                      results (set (repo/query conn q :named-graphs ["http://unpublished-graph-ds1"]))
                      expected (set (map statement->spog unpublished-ds1))]
                  (t/is (= expected results)))))))))))
