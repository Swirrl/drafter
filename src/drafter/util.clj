(ns drafter.util
  (:require [clojure.string :as str]
            [markdown.core :as md]
            [noir.io :as io])
  (:import [org.openrdf.model.impl URIImpl ContextStatementImpl]))

;map-values :: (a -> b) -> Map[k, a] -> Map[k, b]
(defn map-values
  "Maps the values in a map with the given transform function."
  [f m]
  (into {} (for [[k v] m] [k (f v)])))

;;(a -> b) -> Map[a a] -> Map[b b]
(defn map-all
  "Maps both keys and values with the given transform function."
  [f m]
  (into {} (for [[k v] m] [(f k) (f v)])))

(defn to-coll
  "Lifts a non-collection value into a collection containing that
  value. If the input value is already a collection it is returned
  directly. If the input is not a collection, one is created by
  passing the value to coll-f. This defaults to vector if none is
  provided."
  ([x] (to-coll x vector))
  ([x coll-f] (if (or (nil? x)
                      (coll? x))
                x
                (coll-f x))))

(defn construct-dynamic
  "Dynamically creates an instance of a class using the given sequence
  of constructor arguments."
  [klass args]
  (clojure.lang.Reflector/invokeConstructor klass (into-array Object args)))

(defn construct-dynamic*
  "Dyamically creates an instance of a class with the given
  constructor arguments."
  [klass & args]
  (construct-dynamic klass args))

(defn md->html
  "reads a markdown file from public/md and returns an HTML string"
  [filename]
  (->>
    (io/slurp-resource filename)
    (md/md-to-html-string)))

(defn get-causes [ex]
  "Returns a flattened vector containing the root exception and all
  inner cause exceptions."
  (take-while some? (iterate #(.getCause %) ex)))

(defmacro set-var-root! [var form]
  `(alter-var-root ~var (fn [& _#]
                          ~form)))

(defn make-compound-sparql-query
  "Combines a sequence of SPARQL queries into a single query."
  [queries]
  (str/join "; " queries))

(defmacro conj-if
  "Returns (conj col x) if test evaluates to true, otherwise returns
  col."
  [test col x]
  `(if ~test
     (conj ~col ~x)
     ~col))

(defn batch-partition-by
  "Partitions an input sequence into a sequence of batches, partitions
  each batch by the given partition function and flattens the result
  into a sequence of batches where each element is considered equal by
  the partition function. Each of these sequences are then partitioned
  again into batches no more than output-batch-size in length.

  Examples:
  (batch-partition-by [:a :a :a] identity 2 10
  => [[:a :a] [:a]]

  (batch-partition-by [:a :b :a :b] identity 5 10
  => [[:a :a] [:b :b]]

  batch-partition-by [:a :b :a :b :a] identity 2 10
  => [[:a :a] [:a] [:b :b]]

  batch-patition-by [:a :b :a :b :a] identity 2 4
  => [[:a :a] [:b :b] [:a]]"
  ([seq partition-fn output-batch-size] (batch-partition-by seq partition-fn output-batch-size (* 4 output-batch-size)))
  ([seq partition-fn output-batch-size take-batch-size]
   (let [take-batches (partition-all take-batch-size seq)
         grouped-batches (map #(group-by partition-fn %) take-batches)
         batches (mapcat vals grouped-batches)]
     (mapcat #(partition-all output-batch-size %) batches))))

(defn string->sesame-uri [s]
  (URIImpl. s))

(defn seq->iterator
  "Creates a java Iterator for a sequence."
  [s]
  (let [state (atom s)]
    (reify java.util.Iterator
      (hasNext [this] (boolean (seq @state)))
      (next [this]
        (let [value (first @state)]
          (swap! state rest)
          value)))))

(defn seq->iterable
  "Creates a java Iterable implementation for a sequence."
  [s]
  (reify java.lang.Iterable
    (iterator [this] (seq->iterator s))))

;Map[k a] -> Map[k b] -> (a -> b -> c) -> Map[k c]
(defn intersection-with
  "Intersects two maps by their keys and combines corresponding values
  with the given combination function. Returns a new map of combined
  values mapped to their corresponding keys in the input maps."
  [m1 m2 f]
  (let [kvs (map (fn [[k v]]
                   (if (contains? m2 k)
                     [k (f v (get m2 k))]))
                 m1)
        kvs (remove nil? kvs)]
    (into {} kvs)))

(defn make-quad-statement [triple graph]
  (let [s (.getSubject triple)
        p (.getPredicate triple)
        o (.getObject triple)]
    (ContextStatementImpl. s p o graph)))

(defn seq-contains?
  "Returns whether a sequence contains a given value according to =."
  [col value]
  (boolean (some #(= value %) col)))

(defn implies [p q]
  (or (not p) q))
