(ns drafter.feature.draftset.delete
  (:require [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.feature.draftset-data.middleware
             :refer
             [parse-graph-param-handler]]
            [drafter.routes.draftsets-api
             :refer
             [parse-query-param-flag-handler wrap-as-draftset-owner]]
            [drafter.rdf.draftset-management.job-util :as jobs]))

(defn delete-draftset-graph-handler [{backend :drafter/backend wrap-as-draftset-owner :wrap-as-draftset-owner}]
  (wrap-as-draftset-owner
   (parse-query-param-flag-handler
    :silent
    (parse-graph-param-handler
     true
     (fn [{{:keys [draftset-id graph silent]} :params :as request}]
       (if (mgmt/is-graph-managed? backend graph)
         (jobs/run-sync #(dsops/delete-draftset-graph! backend draftset-id graph get-current-time)
                   #(draftset-sync-write-response % backend draftset-id))
         (if silent
           (ring/response (dsops/get-draftset-info backend draftset-id))
           (unprocessable-entity-response (str "Graph not found")))))))))

(defmethod ig/pre-init-spec ::delete-draftset-graph-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::delete-draftset-graph-handler [_ opts]
  (delete-draftset-graph-handler opts))
