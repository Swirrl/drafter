(ns drafter.util
  (:require [markdown.core :as md]
            [noir.io :as io]))

(defn md->html
  "reads a markdown file from public/md and returns an HTML string"
  [filename]
  (->>
    (io/slurp-resource filename)
    (md/md-to-html-string)))
