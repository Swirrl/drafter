(ns drafter.backend.live-test
  (:require [clojure.test :as t]
            [drafter.backend.common :as bcom]
            [drafter.backend.live :as sut]
            [drafter.stasher-test :as stasher-test]
            [drafter.test-common :as tc :refer [deftest-system]]
            [grafter.rdf.protocols :as pr :refer [->Triple]]
            [grafter.rdf4j.repository :as repo])
  (:import java.net.URI))

(def live-graph-1 (URI. "http://live-and-ds1-and-ds2"))
(def live-graph-only (URI. "http://live-only"))

(def construct-graph-query "CONSTRUCT { ?g ?g ?g } WHERE { GRAPH ?g { ?s ?p ?o }}")

(deftest-system endpoint-test-querying
  [{:keys [drafter.backend.live/endpoint
           drafter.stasher/filecache]} "drafter/backend/live-test.edn"]

  (t/testing ":drafter.backend.live/endpoint is both cached and restricted"
    (t/testing "Restricted Endpoint restricts queries to live graphs only"
      (let [preped-query (bcom/prepare-query endpoint construct-graph-query)

            ;; As this test uses a construct :s is indeed :g
            visible-graphs (set (map :s (repo/evaluate preped-query)))]
        (t/is (= #{live-graph-1 live-graph-only}
                 visible-graphs))
        
        (t/testing "Stashes results in a stasher cache"
          (let [expected-triples #{(->Triple live-graph-1 live-graph-1 live-graph-1)
                                   (->Triple live-graph-only live-graph-only live-graph-only)}]
            (stasher-test/assert-cached-results filecache
                                                endpoint
                                                construct-graph-query
                                                (.getDataset preped-query)
                                                expected-triples)))))))

(def all-live-triples #{(->Triple (URI. "http://a") (URI. "http://a") (URI. "http://a"))
                        (->Triple (URI. "http://live-only") (URI. "http://live-only") (URI. "http://live-only"))})

(deftest-system endpoint-test-to-statements
  [{:keys [drafter.backend.live/endpoint
           drafter.stasher/filecache]} "drafter/backend/live-test.edn"]

  (t/testing "to-statements applies live restriction"
    (t/is (= all-live-triples
             (set (pr/to-statements (repo/->connection endpoint) {}))))))
