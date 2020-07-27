(ns drafter.sparql
  (:require [instaparse.core :as i]
            [clojure.java.io :as io]))

(i/defparser parse (io/resource "drafter/sparql.bnf"))
