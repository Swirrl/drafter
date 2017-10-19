(ns drafter.util
  (:require [clojure
             [pprint :as pp]
             [string :as str]]
            [buddy.core.codecs.base64 :as base64]
            [buddy.core.codecs :as codecs])
  (:import java.nio.charset.Charset
           [javax.mail.internet AddressException InternetAddress]
           org.eclipse.rdf4j.model.impl.URIImpl))

(defn str->base64 [s]
  (codecs/bytes->str (base64/encode s)))

(defn base64->str [bs]
  (codecs/bytes->str (base64/decode bs)))

(defn statsd-name
  "Takes any number of strings or keywords and formats them into a
  datadog/statsd style string, i.e. it joins the parts with .'s and
  replaces -'s with _'s"
  [& s]
  (-> (str/join "." (map name s) )
      (str/replace #"-" "_")))

(defmacro log-time-taken
  "Macro that logs the time spent doing something at :info level,
  captures the form execued in the log output."
  {:style/indent :defn}
  [msg & forms]
  (let [md (meta &form)
        line-num (:line md)
        col-num (:column md)
        forms-str (with-out-str (pp/pprint (cons 'do forms)))]
    `(do
       (clojure.tools.logging/debug "About to execute"
                                   (str ~msg " (line #" ~line-num ")")
                                   #_~forms-str)
       (let [start-time# (System/currentTimeMillis)]
           ~@forms
           (let [end-time# (System/currentTimeMillis)
                 execution-time# (- end-time# start-time#)]
             (clojure.tools.logging/info ~msg "took" (str execution-time# "ms") #_~forms-str))))))

;;map-values :: (a -> b) -> Map[k, a] -> Map[k, b]
(defn map-values
  "Maps the values in a map with the given transform function."
  [f m]
  (into {} (for [[k v] m] [k (f v)])))

;;(a -> b) -> Map[a a] -> Map[b b]
(defn map-all
  "Maps both keys and values with the given transform function."
  [f m]
  (into {} (for [[k v] m] [(f k) (f v)])))

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
  (str/join ";\n" queries))

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

(defn uri->sesame-uri
  "Converts a java.net.URI into a sesame URI"
  [uri]
  (URIImpl. (str uri)))

;; Map[k a] -> Map[k b] -> (a -> b -> c) -> Map[k c]
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
  (assoc triple :c graph))

(defn seq-contains?
  "Returns whether a sequence contains a given value according to =."
  [col value]
  (boolean (some #(= value %) col)))

(defn implies [p q]
  (or (not p) q))

(defn merge-in
  "Merges each of the maps with the map at the path defined by ks
  inside the nested target structure."
  [target ks & ms]
  (update-in target ks #(apply merge % ms)))

(defn validate-email-address
  "Validates that a value is a string containing a valid
  representation of an email address. If the string is valid, the
  contained email address is returned as a string. Otherwise false is
  returned."
  [s]
  (and
   (string? s)
   (try
     (let [ia (InternetAddress. s)]
       (.validate ia)
       (.getAddress ia))
     (catch AddressException ex false))))

(def utf8 (Charset/forName "UTF-8"))

(defn throwable? [x]
  (instance? Throwable x))
