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
            [ring.util.response :as ring]
            [drafter.requests :as req]))

(defn remove-graph-from-draftset-handler
  "Remove a supplied graph from the draftset."
  [{:keys [:drafter/backend :drafter/global-writes-lock wrap-as-draftset-owner]}]
  (let [resources {:backend backend :global-writes-lock global-writes-lock}]
    (wrap-as-draftset-owner
     (parse-query-param-flag-handler
      :silent
      (feat-middleware/parse-graph-param-handler
       true
       (fn [{{:keys [draftset-id graph silent]} :params :as request}]
         (if (mgmt/is-graph-managed? backend graph)
           (feat-common/run-sync resources
                                 (req/user-id request)
                                 'delete-draftset-graph
                                 draftset-id
                                 #(dsops/delete-draftset-graph! backend draftset-id graph util/get-current-time)
                                 #(feat-common/draftset-sync-write-response % backend draftset-id))
           (if silent
             (ring/response (dsops/get-draftset-info backend draftset-id))
             (drafter-response/unprocessable-entity-response (str "Graph not found"))))))))))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.delete-by-graph/remove-graph-handler [_]
  (s/keys :req [:drafter/backend :drafter/global-writes-lock]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete-by-graph/remove-graph-handler [_ opts]
  (remove-graph-from-draftset-handler opts))
