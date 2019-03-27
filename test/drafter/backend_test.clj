(ns drafter.backend-test
  (:require [clojure.test :as t]
            [drafter.fixtures.state-1 :as state]
            [drafter.backend :as sut]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.test-common :as tc :refer [deftest-system-with-keys]]
            [grafter-2.rdf4j.repository :as repo])
  (:import java.net.URI))


(deftest-system-with-keys drafter-backend-test [:drafter/backend :drafter.fixture-data/loader]
  [{:keys [:drafter/backend] :as sys} "drafter/backend-test.edn"]

  (t/testing "unrestricted repository"
    (let [conn (repo/->connection backend)]
      (t/is (-> conn
                (repo/query "ASK { GRAPH <http://publishmydata.com/graphs/drafter/drafts> { ?s ?p ?o } }"))
            "We can see the state graph")

      (t/is (-> conn
                (repo/query "ASK { GRAPH <http://live-only> { ?s ?p ?o } }"))
            "We can see live graphs")))

  (t/testing "endpoint-repo"
    (t/testing "getting just the live/public data"
      (let [live-endpoint (sut/endpoint-repo backend ::sut/live {})

            visible-graphs (set (map :g (repo/query (repo/->connection live-endpoint)
                                                    "SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o }}")))]
        (t/is (= state/expected-live-graphs
                 visible-graphs))))


    (t/testing "getting draftset repo"
      (let [ds1-endpoint (sut/endpoint-repo backend state/ds-1 {:union-with-live true})]
        (t/is (satisfies? repo/ToConnection ds1-endpoint))

        ;; TODO: https://github.com/Swirrl/drafter/issues/219
        #_(let [visible-graphs (set (map :g (repo/query (repo/->connection ds1-endpoint)
                                                      "SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o }}")))]
          (t/is (= state/expected-live-graphs
                   visible-graphs)))))))
