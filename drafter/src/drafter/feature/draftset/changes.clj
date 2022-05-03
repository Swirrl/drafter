(ns drafter.feature.draftset.changes
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [drafter.feature.middleware :as middleware]
            [drafter.rdf.draftset-management.job-util :as jobutil]
            [integrant.core :as ig]
            [ring.util.response :as ring]
            [drafter.responses :as response]
            [drafter.requests :as req]
            [drafter.rdf.drafter-ontology :refer [modified-times-graph-uri]]
            [drafter.feature.modified-times :as modified-times]
            [drafter.time :as time]))

(defn revert-graph-changes!
  "Reverts the changes made to a live graph inside the given
  draftset. Returns a result indicating the result of the operation:
    - :reverted If the changes were reverted
    - :not-found If the draftset does not exist or no changes exist within it."
  [{:keys [backend graph-manager clock] :as manager} draftset-ref graph]
  (if-let [draft-graph-uri (dsops/find-draftset-draft-graph backend draftset-ref graph)]
    (do
      (mgmt/delete-draft-graph! backend draft-graph-uri)
      (modified-times/draft-graph-reverted! backend graph-manager draftset-ref draft-graph-uri (time/now clock))
      :reverted)
    :not-found))

(defn delete-draftset-changes-handler
  [{:keys [wrap-as-draftset-owner] {:keys [backend] :as manager} :drafter/manager}]
  (wrap-as-draftset-owner :draft:edit
   (middleware/parse-graph-param-handler
    true
    (fn [{{:keys [draftset-id graph]} :params :as request}]
      (feat-common/run-sync
       manager
       (req/user-id request)
       'delete-draftset-changes
       draftset-id
       #(revert-graph-changes! manager draftset-id graph)
       (fn [result]
         (if (jobutil/failed-job-result? result)
           (response/api-response 500 result)
           (if (= :reverted (:details result))
             (ring/response (dsops/get-draftset-info backend draftset-id))
             (ring/not-found "")))))))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.changes/delete-changes-handler [_]
  (s/keys :req [:drafter/manager]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset.changes/delete-changes-handler [_ opts]
  (delete-draftset-changes-handler opts))
