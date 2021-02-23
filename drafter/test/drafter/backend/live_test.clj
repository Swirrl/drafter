(ns drafter.backend.live-test
  (:require [clojure.test :as t]
            [drafter.fixtures.state-1 :as state-1]
            [drafter.stasher-test :as stasher-test]
            [drafter.test-common :as tc :refer [deftest-system]]
            [drafter.rdf.drafter-ontology :refer [modified-times-graph-uri]]
            [grafter-2.rdf.protocols :as pr :refer [->Triple]]
            [grafter-2.rdf4j.repository :as repo]
            [grafter-2.rdf4j.io :as rio]
            [clojure.java.io :as io])
  (:import java.net.URI))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def live-graph-1 (URI. "http://live-and-ds1-and-ds2"))
(def live-graph-only (URI. "http://live-only"))

(def construct-graph-query "CONSTRUCT { ?g ?g ?g } WHERE { GRAPH ?g { ?s ?p ?o }}")

(t/deftest endpoint-test-prepare-query
  (tc/with-system
    [{:keys [drafter.backend.live/endpoint
             drafter.stasher/cache]
      repo  :drafter.stasher/repo} "drafter/backend/live-test.edn"]
    (t/testing ":drafter.backend.live/endpoint is both cached and restricted"
      (t/testing "Restricted Endpoint restricts queries to live graphs only"
        (let [preped-query (repo/prepare-query (repo/->connection endpoint) construct-graph-query)
              ;; As this test uses a construct :s is indeed :g
              visible-graphs (set (map :s (repo/evaluate preped-query)))]
          (t/is (= state-1/expected-live-graphs visible-graphs))
          (t/testing "Stashes results in a stasher cache"
            (let [expected-triples (set (map (fn [g] (pr/->Triple g g g)) state-1/expected-live-graphs))]
              (stasher-test/assert-cached-results cache
                                                  repo
                                                  construct-graph-query
                                                  (.getDataset preped-query)
                                                  expected-triples))))))))

(t/deftest endpoint-test-query
  (tc/with-system
    [{:keys [drafter.backend.live/endpoint]} "drafter/backend/live-test.edn"]
    (t/testing ":drafter.backend.live/endpoint"
      (t/testing "query-dataset interface restricts queries to live graphs only"
        (let [results (repo/query (repo/->connection endpoint) construct-graph-query)
              ;; As this test uses a construct :s is indeed :g
              visible-graphs (set (map :s results))]
          (t/is (= state-1/expected-live-graphs visible-graphs)))))))

(t/deftest endpoint-test-to-statements
  (tc/with-system
    [{:keys [drafter.backend.live/endpoint
             drafter.stasher/filecache]} "drafter/backend/live-test.edn"]

    (t/testing "to-statements applies live restriction"
      (let [expected-triples (->> (rio/statements (io/resource "drafter/stasher-test/drafter-state-1.trig"))
                                  (filter (fn [quad] (contains? state-1/expected-live-graphs (pr/context quad))))
                                  (map pr/map->Triple)
                                  (set))
            stmts (with-open [conn (repo/->connection endpoint)]
                    (set (pr/to-statements conn {:grafter.repository/infer false})))]
        (t/is (= expected-triples stmts))))))
