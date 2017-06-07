(ns drafter.routes.sparql
  (:require [compojure.core :refer [make-route]]
            [drafter.rdf
             [endpoints :refer [live-endpoint]]
             [sparql-protocol :refer [sparql-end-point sparql-protocol-handler]]]))

(defn live-sparql-routes [mount-point executor query-timeout-fn]
  (sparql-end-point mount-point (live-endpoint executor) query-timeout-fn))
