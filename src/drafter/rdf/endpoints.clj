(ns drafter.rdf.endpoints
  (:require [drafter.backend.endpoints :refer [create-restricted]]
            [drafter.rdf.draft-management :as mgmt]))

(defn live-endpoint
  "Creates a backend restricted to the live graphs."
  [backend]
  (create-restricted backend (partial mgmt/live-graphs backend)))

(defn state-endpoint
  "Creates a backend restricted to the state graph."
  [backend]
  (create-restricted backend #{mgmt/drafter-state-graph}))
