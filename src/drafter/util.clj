(ns drafter.util
  (:require [markdown.core :as md]
            [noir.io :as io]))

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
