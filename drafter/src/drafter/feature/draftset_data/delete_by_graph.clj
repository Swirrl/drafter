(ns drafter.feature.draftset-data.delete-by-graph
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [drafter.feature.middleware :as feat-middleware]
            [drafter.responses :as drafter-response]
            [drafter.routes.draftsets-api
             :refer
             [parse-query-param-flag-handler]]
            [drafter.util :as util]
            [integrant.core :as ig]
            [ring.util.response :as ring]))

(defn remove-graph-from-draftset-handler
  "Remove a supplied graph from the draftset."
  [{backend :drafter/backend wrap-as-draftset-owner :wrap-as-draftset-owner}]
  (wrap-as-draftset-owner
   (parse-query-param-flag-handler
    :silent
    (feat-middleware/parse-graph-param-handler
     true
     (fn [{{:keys [draftset-id graph silent]} :params :as request}]
       (if (mgmt/is-graph-managed? backend graph)
         (feat-common/run-sync #(dsops/delete-draftset-graph! backend draftset-id graph util/get-current-time)
                               #(feat-common/draftset-sync-write-response % backend draftset-id))
         (if silent
           (ring/response (dsops/get-draftset-info backend draftset-id))
           (drafter-response/unprocessable-entity-response (str "Graph not found")))))))))

(defmethod ig/pre-init-spec ::remove-graph-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::remove-graph-handler [_ opts]
  (remove-graph-from-draftset-handler opts))
