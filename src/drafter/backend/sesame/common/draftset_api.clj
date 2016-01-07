(ns drafter.backend.sesame.common.draftset-api
  (:require [drafter.backend.protocols :as backend]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.draftset-management :as dsmgmt]))

(defn- publish-draftset-graphs-joblet [backend draftset-ref]
  (jobs/action-joblet
   (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-ref)]
     (backend/migrate-graphs-to-live! backend (vals graph-mapping)))))

(defn- delete-draftset-joblet [backend draftset-ref]
  (jobs/action-joblet
   (dsmgmt/delete-draftset-statements! backend draftset-ref)))

(defn publish-draftset-job [backend draftset-ref]
  (jobs/joblet-seq->job [(publish-draftset-graphs-joblet backend draftset-ref)
                         (delete-draftset-joblet backend draftset-ref)] :batch-write))
