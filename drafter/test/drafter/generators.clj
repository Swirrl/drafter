(ns drafter.generators
  (:require [clojure.test.check.generators :as gen]
            [grafter-2.rdf.protocols :as pr])
  (:import [java.net URI]))

(def graph-gen (gen/fmap (fn [id]
                           (URI. (str "http://g/" id))) gen/uuid))

(def subject-gen (gen/fmap (fn [id]
                             (URI. (str "http://s/" id))) gen/uuid))

(def predicate-gen (gen/fmap (fn [id]
                               (URI. (str "http://p/" id))) gen/uuid))

(def object-uri-gen (gen/fmap (fn [id]
                                (URI. (str "http://o/" id))) gen/uuid))

(def object-gen (gen/one-of [object-uri-gen
                             gen/string-alphanumeric
                             gen/int
                             gen/boolean]))

(defn triple-gen
  "Returns a generator for random triples. The optioanl gens map can be used to specify
   the generator to use for the s,p.o fields of the generated triples"
  ([] (triple-gen {}))
  ([gens]
   (let [default-gens {:s subject-gen
                       :p predicate-gen
                       :o object-gen}
         gens (merge default-gens gens)]
     (gen/fmap pr/map->Triple (apply gen/hash-map (mapcat identity gens))))))

(defn quad-gen
  "Returns a generator for random quads. The optional gens map can be used to specify the
   generator to use for the s,p,o,c fields of the generated quads."
  ([] (quad-gen {}))
  ([gens]
   (let [default-gens {:s subject-gen
                       :p predicate-gen
                       :o object-gen
                       :c graph-gen}
         gens (merge default-gens gens)]
     (gen/fmap pr/map->Quad (apply gen/hash-map (mapcat identity gens))))))

(defn random-graph-uri
  "Returns a random graph URI"
  []
  (gen/generate graph-gen))

(defn generate-graph-triples
  "Generates the specified number of triples within a graph"
  [graph-uri n]
  (let [graph-gen (gen/return graph-uri)
        qg (quad-gen {:c graph-gen
                      ;; NOTE: generate some self-referential objects which are subject to rewriting
                      :o (gen/one-of [graph-gen object-gen])})]
    (gen/generate (gen/vector qg n))))

(defn generate-quads
  "Generates the specified number of random quads"
  [n]
  (let [graph-uri (random-graph-uri)]
    (generate-graph-triples graph-uri n)))

(defn generate-triples
  "Generates random triples"
  ([] (generate-triples 1 10))
  ([count] (-> (gen/vector (triple-gen) count) (gen/generate)))
  ([min max]
   (-> (gen/vector (triple-gen) min max)
       (gen/generate))))


