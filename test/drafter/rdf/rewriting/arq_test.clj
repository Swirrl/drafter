(ns drafter.rdf.rewriting.arq-test
  "Testing round tripping through Jena ARC"
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [drafter.rdf.rewriting.arq :as sut]))

(defn load-query [res-path]
  (slurp (io/resource res-path)))

(defn round-trip-query-string [qs]
  (-> qs
      sut/sparql-string->arq-query
      sut/->sse-item
      sut/->sparql-string))

(defn roundtripable?
  "Some crude round trip tests.  These might not be true for all
  possible queries."
  [qs]
  (let [qs-rt (round-trip-query-string qs)
        arc-qs (sut/sparql-string->arq-query qs)
        arc-rt (sut/sparql-string->arq-query qs-rt)

        sse-qs (sut/->sse-item arc-qs)
        sse-rt (sut/->sse-item arc-rt)]

    (t/is (= sse-qs
             sse-rt)
          "SSE's are equal after round trip")

    (t/is (= qs-rt
             (round-trip-query-string qs-rt))
          "Round tripping twice generates same query string")))

(t/deftest rewriting-with-sparql-group-concats
  (t/is (roundtripable? (load-query "test-queries/group-concat.sparql"))))
