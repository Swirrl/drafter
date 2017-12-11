(ns drafter.backend-test
  (:require [clojure.test :as t]
            [drafter.backend :as sut]
            [drafter.test-common :as tc :refer [deftest-system-with-keys]]
            [grafter.rdf4j.repository :as repo])
  (:import java.net.URI))

(def expected-live-graphs #{(URI. "http://live-and-ds1-and-ds2")
                            (URI. "http://live-only")})

(deftest-system-with-keys drafter-backend-test [:drafter/backend :drafter.fixture-data/loader]
  [{:keys [:drafter/backend] :as sys} "drafter/backend-test.edn"]
  (let [live-endpoint (sut/endpoint-repo backend ::sut/live {})]
    
    (let [visible-graphs (set (map :g (repo/query (repo/->connection live-endpoint)
                                                  "SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o }}")))]
      (t/is (= expected-live-graphs
               visible-graphs)))))

