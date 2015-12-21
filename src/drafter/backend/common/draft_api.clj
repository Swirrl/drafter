(ns drafter.backend.common.draft-api
  (:require [drafter.backend.protocols :as backend]
            [drafter.rdf.draft-management.jobs :as jobs]
            [grafter.rdf :refer [context]]
            [grafter.rdf.protocols :refer [map->Triple]]))

(defn migrate-graphs-to-live-job
  "Default implementation of migrate-graphs-to-live-job."
  [backend graphs]
  (jobs/make-job :exclusive-write [job]
                 (backend/migrate-graphs-to-live! backend graphs)
                 (jobs/job-succeeded! job)))

(defn quad-batch->graph-triples
  "Extracts the graph-uri from a sequence of quads and converts all
  quads into triples. Expects each quad in the sequence to have the
  same target graph."
  [quads]
  (if-let [graph-uri (context (first quads))]
    {:graph-uri graph-uri :triples (map map->Triple quads)}
    (throw (IllegalArgumentException. "Quad batch must contain at least one item"))))
