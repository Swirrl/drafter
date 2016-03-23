(ns drafter.backend.common.draft-api
  (:require [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.draft-management.jobs :as jobs]
            [grafter.rdf :refer [context]]
            [grafter.rdf.protocols :refer [map->Triple]]))

(defn quad-batch->graph-triples
  "Extracts the graph-uri from a sequence of quads and converts all
  quads into triples. Expects each quad in the sequence to have the
  same target graph."
  [quads]
  (if-let [graph-uri (context (first quads))]
    {:graph-uri graph-uri :triples (map map->Triple quads)}
    (throw (IllegalArgumentException. "Quad batch must contain at least one item"))))
