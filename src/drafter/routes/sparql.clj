(ns drafter.routes.sparql
  (:require [drafter.rdf.sparql-protocol :refer [sparql-end-point]]
            [drafter.rdf.endpoints :refer [live-endpoint state-endpoint]]
            [swirrl-server.responses :as r]))

(defn live-sparql-routes [mount-point executor timeouts]
  (sparql-end-point mount-point (live-endpoint executor) timeouts))

(defn state-sparql-routes [mount-point executor timeouts]
  (sparql-end-point mount-point (state-endpoint executor) timeouts))

(defn raw-sparql-routes [mount-point executor timeouts]
  (sparql-end-point mount-point executor timeouts))
