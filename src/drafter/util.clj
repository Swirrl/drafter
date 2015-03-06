(ns drafter.util
  (:require [noir.io :as io]
            [markdown.core :as md]))

;map-values :: (a -> b) -> Map[k, a] -> Map[k, b]
(defn map-values
  "Maps the values in a map with the given transform function."
  [f m]
  (into {} (for [[k v] m] [k (f v)])))

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
