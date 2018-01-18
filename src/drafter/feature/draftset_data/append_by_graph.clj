(ns drafter.feature.draftset-data.append-by-graph
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as ops]
            [drafter.feature.middleware :as middleware]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.responses :as response :refer [submit-async-job!]]
            [drafter.routes.draftsets-api :refer [wrap-as-draftset-owner]]
            [drafter.util :as util]
            [integrant.core :as ig]
            [drafter.feature.draftset-data.common :as ds-data-common]))

(defn create-or-empty-draft-graph-for [backend draftset-ref live-graph clock-fn]
  (if-let [draft-graph-uri (ops/find-draftset-draft-graph backend draftset-ref live-graph)]
    (do
      (mgmt/delete-graph-contents! backend draft-graph-uri (clock-fn))
      draft-graph-uri)
    (mgmt/create-draft-graph! backend live-graph draftset-ref clock-fn)))


(defn copy-live-graph-into-draftset-job [backend draftset-ref live-graph]
  (jobs/make-job :background-write [job]
                 (let [draft-graph-uri (create-or-empty-draft-graph-for backend draftset-ref live-graph util/get-current-time)]
                   (ds-data-common/lock-writes-and-copy-graph backend live-graph draft-graph-uri {:silent true})
                   (jobs/job-succeeded! job))))



(defn- required-live-graph-param-handler [backend inner-handler]
  (fn [{{:keys [graph]} :params :as request}]
    (if (mgmt/is-graph-live? backend graph)
      (inner-handler request)
      (response/unprocessable-entity-response (str "Graph not found in live")))))

(defn put-draftset-graph-handler [{backend :drafter/backend
                                   :keys [wrap-as-draftset-owner]}]
  (letfn [(required-live-graph-param [handler]
            (middleware/parse-graph-param-handler true (required-live-graph-param-handler backend handler)))]
    
    (wrap-as-draftset-owner
     (required-live-graph-param
      (fn [{{:keys [draftset-id graph]} :params}]
        (submit-async-job! (copy-live-graph-into-draftset-job backend draftset-id graph)))))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::handler [_ opts]
  (put-draftset-graph-handler opts))
