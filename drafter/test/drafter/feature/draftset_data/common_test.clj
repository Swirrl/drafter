(ns ^:rest-api drafter.feature.draftset-data.common-test
  (:require [clojure.test :as t]
            [drafter.feature.draftset-data.common :as sut]
            [drafter.test-common :as tc]
            [grafter-2.rdf.protocols :refer [->Quad triple=]])
  (:import java.net.URI))

(t/use-fixtures :each tc/with-spec-instrumentation)

(t/deftest quad-batch->graph-triples-test
  (t/testing "Batch quads have nil graph"
    (let [quads [(->Quad (URI. "http://s1") (URI. "http://p1") "o1" nil)
                 (->Quad (URI. "http://s2") (URI. "http://p2") "o2" nil)]]
      (t/is (thrown? IllegalArgumentException (sut/quad-batch->graph-triples quads)))))

  (t/testing "Non-empty batch"
    (let [guri "http://graph"
          quads (map #(->Quad (str "http://s" %) (str "http://p" %) (str "http://o" %) guri) (range 1 10))
          {:keys [graph-uri triples]} (sut/quad-batch->graph-triples quads)]
      (t/is (= guri graph-uri))
      (t/is (every? identity (map triple= quads triples))))))
