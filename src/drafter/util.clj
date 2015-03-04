(ns drafter.util
  (:require [noir.io :as io]
            [markdown.core :as md]))

;map-values :: (a -> b) -> Map[k, a] -> Map[k, b]
(defn map-values
  "Maps the values in a map with the given transform function."
  [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn md->html
  "reads a markdown file from public/md and returns an HTML string"
  [filename]
  (->>
    (io/slurp-resource filename)
    (md/md-to-html-string)))
