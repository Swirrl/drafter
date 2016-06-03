(ns drafter.util
  (:require [clojure.string :as str]
            [markdown.core :as md]
            [clojure.pprint :as pp]
            [noir.io :as io]))

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
       (clojure.tools.logging/info "About to execute"
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
