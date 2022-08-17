(ns data-gen.core
  (:require [clojure.test.check.generators :as gen]
            [grafter-2.rdf.protocols :as pr])
  (:import [java.net URI]
           [java.time LocalDate]
           [java.time.temporal ChronoUnit]))

(defn uri-gen [prefix]
  (gen/fmap (fn [uuid] (URI. (str prefix uuid))) gen/uuid))

(def graph-gen (uri-gen "http://example.com/graphs/"))
(def subject-gen (uri-gen "http://example.com/subject/"))
(def predicate-gen (uri-gen "http://example.com/predicate/"))

(def string-gen gen/string-alphanumeric)
(def lang-gen (gen/elements [:en :es :fr :de]))
(def lang-string-gen (gen/fmap (fn [[s lang]] (pr/language s lang)) (gen/tuple string-gen lang-gen)))

(defn date-gen
  ([]
   (let [today (LocalDate/now)]
     (date-gen (.minusWeeks today 2) today)))
  ([^LocalDate min]
   (date-gen min (.plusWeeks min 2)))
  ([^LocalDate min ^LocalDate max]
   (let [days-until (.until min max ChronoUnit/DAYS)]
     (gen/fmap (fn [days] (.plusDays min days)) (gen/choose 0 (inc days-until))))))

(def object-gen (gen/one-of [string-gen
                             lang-string-gen
                             gen/boolean
                             gen/int
                             gen/double
                             (date-gen)
                             (uri-gen "http://example.com/object/")]))

(defn quad-gen
  ([] (quad-gen {}))
  ([{:keys [s p o g] :or {s subject-gen
                          p predicate-gen
                          o object-gen
                          g graph-gen}}]
   (gen/fmap (fn [vs] (apply pr/->Quad vs)) (gen/tuple s p o g))))

(defn graph-quad-gen
  "Returns a generator for quads within a given graph. Also takes a
  collection of all the graph URIs in the dataset to generate
  graph-referencing values."
  [graph-uri graphs]
  (let [graph-ref-gen (gen/elements graphs)]
    (quad-gen {:g (gen/return graph-uri)
               :s (gen/frequency [[1 graph-ref-gen] [9 subject-gen]])
               :p (gen/frequency [[1 graph-ref-gen] [9 predicate-gen]])
               :o (gen/frequency [[1 graph-ref-gen] [9 object-gen]])})))

(defn non-ref-quad-gen
  "Generates a quad in the given graph which does not reference any
  other graphs"
  [graph-uri]
  (quad-gen {:g (gen/return graph-uri)
             :s subject-gen
             :p predicate-gen
             :o object-gen}))

(defn- sub-map-gen
  "Generates non-empty maps where the keys are chosen from
  candidate-keys and the corresponding values are all v"
  [candidate-keys v]
  (gen/fmap (fn [ks]
              (zipmap ks (repeat v)))
            (gen/set (gen/elements candidate-keys) {:min-elements 1 :max-elements (count candidate-keys)})))

(defn graph-ref-quad-gen
  "Generates a quad in the graph graph-uri which references at least one
  of the graphs in graphs in the subject, predicate or object
  positions."
  [graph-uri graphs]
  (let [graph-ref-gen (gen/elements graphs)
        default-gens {:g (gen/return graph-uri)
                      :s subject-gen
                      :p predicate-gen
                      :o object-gen}]
    (gen/bind (sub-map-gen [:s :p :o] graph-ref-gen)
              (fn [m] (quad-gen (merge default-gens m))))))

(defn buckets-gen [n buckets]
  (letfn [(size-buckets [n avg-bucket-size remaining-indices acc]
            (cond
              ;; add all remaining items to the last bucket
              (= 1 (count remaining-indices))
              (let [idx (first remaining-indices)]
                (gen/return (update acc idx + n)))

              ;; done if no more elements remaining
              (zero? n)
              (gen/return acc)

              ;; choose a random remaining bucket to update
              :else
              (let [bucket-upper (min n (* 2 avg-bucket-size))]
                (gen/let [idx (gen/elements remaining-indices)
                          size (gen/choose 0 bucket-upper)]
                  (size-buckets (- n size) avg-bucket-size (disj remaining-indices idx) (update acc idx + size))))))
          (choose-buckets [n acc]
            (if (zero? n)
              (gen/return acc)
              (gen/let [bucket-idx (gen/choose 0 (count acc))]
                (choose-buckets (dec n) (update acc bucket-idx inc)))))]
    ;; if there are more buckets than distribute, distribute items by
    ;; choosing a bucket to place them in. Otherwise choose a random
    ;; size for each bucket based on the average bucket size.
    (if (> buckets n)
      (choose-buckets n (vec (repeat buckets 0)))
      (let [avg-bucket-size (quot n buckets)
            remaining-indices (set (range buckets))
            counts (vec (repeat buckets 0))]
        (size-buckets n avg-bucket-size remaining-indices counts)))))

(defn sample-1 [g]
  (first (gen/sample g 1)))

(defn generate-n [g n]
  (let [batch-size 100000]
    (if (<= n batch-size)
      (gen/generate (gen/vector g n) 200)
      (concat (gen/generate (gen/vector g batch-size) 200) (lazy-seq (generate-n g (- n batch-size))) (lazy-cat)))))

(defn generate-graphs [n]
  (take n (map (fn [i] (URI. (str "http://example.com/graphs/" (inc i)))) (range n))))
