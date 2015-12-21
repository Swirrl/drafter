(ns drafter.backend.common.draft-api-test
  (:require [clojure.test :refer :all]
            [drafter.backend.common.draft-api :refer :all]
            [grafter.rdf :refer [triple=]]
            [grafter.rdf.protocols :refer [->Quad]]))

(deftest quad-batch->graph-triples-test
  (testing "Empty batch"
    (is (thrown? IllegalArgumentException (quad-batch->graph-triples []))))

  (testing "Non-empty batch"
    (let [guri "http://graph"
          quads (map #(->Quad (str "http://s" %) (str "http://p" %) (str "http://o" %) guri) (range 1 10))
          {:keys [graph-uri triples]} (quad-batch->graph-triples quads)]
      (is (= guri graph-uri))
      (is (every? identity (map triple= quads triples))))))
