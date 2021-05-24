(ns drafter.rdf.draftset-management.jobs
  (:require
   [drafter.backend.common :refer :all]
   [drafter.backend.draftset.operations :as ops]
   [drafter.backend.draftset.rewrite-result :refer [rewrite-statement]]
   [drafter.draftset :as ds]
   [drafter.feature.endpoint.public :as pub]
   [drafter.rdf.draftset-management.job-util :as jobs]
   [drafter.rdf.sesame :refer [read-statements]]
   [grafter-2.rdf.protocols :as rdf :refer [context map->Quad]]
   [grafter-2.rdf4j.io :refer [quad->backend-quad]]))

(defn delete-draftset-job [backend user-id {:keys [draftset-id metadata]}]
  (jobs/make-job user-id
                 :background-write
                 (jobs/job-metadata backend draftset-id 'delete-draftset metadata)
                 (fn [job]
                   (ops/delete-draftset! backend draftset-id)
                   (jobs/job-succeeded! job))))

(defn publish-draftset-job
  "Return a job that publishes the graphs in a draftset to live and
  then deletes the draftset."
  [backend user-id {:keys [draftset-id metadata]} clock]
  ;; TODO combine these into a single job as priorities have now
  ;; changed how these will be applied.

  (jobs/make-job user-id
                 :publish-write
                 (jobs/job-metadata backend draftset-id 'publish-draftset metadata)
                 (fn [job]
                   (try
                     (ops/publish-draftset-graphs! backend draftset-id clock)
                     (ops/update-public-endpoint-modified-at! backend)
                     (ops/update-public-endpoint-version! backend)
                     (ops/delete-draftset-statements! backend draftset-id)
                     (jobs/job-succeeded! job)
                     (catch Exception ex
                       (jobs/job-failed! job ex))))))
