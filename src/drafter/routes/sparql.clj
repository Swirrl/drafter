(ns drafter.routes.sparql
  (:require [compojure.core :refer [make-route]]
            [drafter.middleware :refer [require-user-role]]
            [drafter.rdf
             [endpoints :refer [live-endpoint]]
             [sparql-protocol :refer [sparql-end-point sparql-protocol-handler]]]))

(defn live-sparql-routes [mount-point executor query-timeout-fn]
  (sparql-end-point mount-point (live-endpoint executor) query-timeout-fn))

(defn raw-sparql-routes [mount-point executor query-timeout-fn authenticated-fn]
  (->> (sparql-protocol-handler executor query-timeout-fn)
       (require-user-role :system)
       (authenticated-fn)
       (make-route nil mount-point)))
