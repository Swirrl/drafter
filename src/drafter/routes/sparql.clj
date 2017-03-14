(ns drafter.routes.sparql
  (:require [drafter.rdf.sparql-protocol :refer [sparql-end-point sparql-protocol-handler]]
            [drafter.rdf.endpoints :refer [live-endpoint]]
            [compojure.core :refer [make-route]]
            [drafter.middleware :refer [require-user-role]]))

(defn live-sparql-routes [mount-point executor timeouts]
  (sparql-end-point mount-point (live-endpoint executor) timeouts))
