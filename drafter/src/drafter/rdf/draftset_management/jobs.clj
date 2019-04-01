(ns drafter.rdf.draftset-management.jobs
  (:require [drafter.backend.common :refer :all]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as ops]
            [drafter.backend.draftset.rewrite-result :refer [rewrite-statement]]
            [drafter.draftset :as ds]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sesame :refer [read-statements]]
            [drafter.rdf.sparql :as sparql]
            [drafter.util :as util]
            [drafter.write-scheduler :as writes]
            [grafter-2.rdf.protocols :as rdf :refer [context map->Quad]]
            [grafter-2.rdf4j.io :refer [quad->backend-quad]]
            [grafter-2.rdf4j.repository :as repo]
            [grafter.vocabularies.dcterms :refer [dcterms:modified]]
            [swirrl-server.async.jobs :as ajobs])
  (:import java.util.Date
           org.eclipse.rdf4j.model.Resource))

(defn delete-draftset-job [backend draftset-ref]
  (jobs/make-job
    :background-write [job]
    (do (ops/delete-draftset! backend draftset-ref)
        (jobs/job-succeeded! job))))

;; (defn touch-graph-in-draftset [draftset-ref draft-graph-uri modified-at]
;;   (let [update-str (str (mgmt/set-timestamp draft-graph-uri dcterms:modified modified-at) " ; "
;;                         (mgmt/set-timestamp (ds/->draftset-uri draftset-ref) dcterms:modified modified-at))]
;;     update-str))

;; (defn touch-graph-in-draftset! [backend draftset-ref draft-graph-uri modified-at]
;;   (sparql/update! backend
;;                   (touch-graph-in-draftset draftset-ref draft-graph-uri modified-at)))

(defn publish-draftset-job
  "Return a job that publishes the graphs in a draftset to live and
  then deletes the draftset."
  [backend draftset-ref clock-fn]
  ;; TODO combine these into a single job as priorities have now
  ;; changed how these will be applied.

  (jobs/make-job :publish-write [job]
                 (try
                   (ops/publish-draftset-graphs! backend draftset-ref clock-fn)
                   (ops/delete-draftset-statements! backend draftset-ref)
                   (jobs/job-succeeded! job)
                   (catch Exception ex
                     (jobs/job-failed! job ex)))))
