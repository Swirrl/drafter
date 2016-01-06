(ns drafter.backend.sesame.common.draftset-api
  (:require [drafter.backend.protocols :as backend]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.draftset-management :as dsmgmt]))

(defn- publish-draftset-graphs-joblet [backend draftset-uri]
  (jobs/action-joblet
   (let [graph-mapping (dsmgmt/get-draftset-graph-mapping backend draftset-uri)]
     (backend/migrate-graphs-to-live! backend (vals graph-mapping)))))

(defn- delete-draftset-joblet [backend draftset-uri]
  (jobs/action-joblet
   (dsmgmt/delete-draftset-statements! backend draftset-uri)))

(defn publish-draftset-job [backend draftset-id]
  (let [draftset-uri (drafter.rdf.drafter-ontology/draftset-uri draftset-id)]
    (jobs/joblet-seq->job [(publish-draftset-graphs-joblet backend draftset-uri)
                           (delete-draftset-joblet backend draftset-uri)] :batch-write)))
