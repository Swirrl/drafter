(ns drafter.backend.common.draft-api
  (:require [drafter.backend.protocols :as backend]
            [drafter.rdf.draft-management.jobs :as jobs]))

(defn migrate-graphs-to-live-job
  "Default implementation of migrate-graphs-to-live-job."
  [backend graphs]
  (jobs/make-job :exclusive-write [job]
                 (backend/migrate-graphs-to-live! backend graphs)
                 (jobs/job-succeeded! job)))
