(ns drafter.util
  (:require
   [buddy.core.codecs :as codecs]
   [buddy.core.codecs.base64 :as base64]
   [clojure.pprint :as pp]
   [clojure.string :as str]
   [drafter.rdf.drafter-ontology :refer [drafter:version]]
   [drafter.rdf.jena :as jena]
   [grafter-2.rdf.protocols :as pr]
   [grafter.url :as url])
  (:import
   [java.util UUID]
   [javax.mail.internet AddressException InternetAddress]
   java.nio.charset.Charset
   java.security.MessageDigest
   org.apache.commons.codec.binary.Hex))

(defn create-uuid
  "Function that creates a UUID"
  []
  (UUID/randomUUID))

(defn version
  ([]
   (version (create-uuid)))
  ([id]
   (url/->java-uri (url/append-path-segments drafter:version id))))

(defn version->str [v]
  (re-find
   #"(?<=^http://publishmydata.com/def/drafter/version/)[0-9a-f-]+$"
   (str v)))

(defn merge-versions [& vs]
  (->> vs
       (map version->str)
       sort
       (apply str)
       .getBytes
       (.digest (MessageDigest/getInstance "MD5"))
       Hex/encodeHexString
       version))

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

(defn- create-partition-batches
  "Given a sequence of batches, creates an equivalence partition by partition-fn within each batch
  and then partitions each equivalence partition by output-batch-size. Processes each batch within
  take-batches lazily."
  [take-batches partition-fn output-batch-size]
  (lazy-seq
    (when (seq take-batches)
      (let [take-batch (first take-batches)
            batch-groups (vals (group-by partition-fn take-batch))
            batches (mapcat (fn [inner]
                      (partition-all output-batch-size inner))
                    batch-groups)]
        (concat batches (create-partition-batches (rest take-batches) partition-fn output-batch-size))))))

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
  ([seq partition-fn output-batch-size]
    (batch-partition-by seq partition-fn output-batch-size (* 4 output-batch-size)))
  ([seq partition-fn output-batch-size take-batch-size]
   (create-partition-batches (partition-all take-batch-size seq) partition-fn output-batch-size)))



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

(defn make-quad
  "Creates a quad from a graph and triple. Will overwrite
  any existing :c context vals if present in `triple`"
  [graph triple]
  (pr/map->Quad (assoc triple :c graph)))

(defn make-quads
  "Returns a sequence of quads from a graph and sequence of triples"
  [graph triples]
  (map #(make-quad graph %) triples))

(defn quads->insert-data-query
  "Returns an INSERT DATA update query for a set of quads"
  [quads]
  (jena/->update-string [(jena/insert-data-stmt quads)]))

(defn quads->delete-data-query
  "Returns a DELETE DATA update query for a set of quads"
  [quads]
  (jena/->update-string [(jena/delete-data-stmt quads)]))

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
