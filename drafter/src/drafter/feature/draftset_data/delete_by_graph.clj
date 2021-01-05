(ns drafter.feature.draftset-data.delete-by-graph
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [drafter.feature.middleware :as feat-middleware]
            [drafter.responses :as drafter-response]
            [drafter.routes.draftsets-api :refer [parse-query-param-flag-handler]]
            [drafter.backend.draftset.graphs :as graphs]
            [integrant.core :as ig]
            [ring.util.response :as ring]
            [drafter.async.responses :as async-responses]
            [drafter.requests :as req]
            [drafter.rdf.draftset-management.job-util :as job-util]))

(defn delete-graph
  "Deletes the given live graph within a draftset. Returns the draftset summary for the
   new draft state. The graph to delete must exist in the live graph unless silent? is
   false. An exception will be thrown if silent? is falsey and the graph does not exist,
   or if the delete operation fails for some reason. In these cases the ex-info of the
   exception will be a map containing a :type key indicating the reason for the error."
  [{:keys [backend graph-manager] :as manager} user-id draftset-ref graph silent?]
  (cond
    (mgmt/is-graph-managed? backend graph)
    (do
      (let [job-result (feat-common/run-sync manager
                                             user-id
                                             'delete-draftset-graph
                                             draftset-ref
                                             #(graphs/delete-user-graph graph-manager draftset-ref graph)
                                             identity)]
        (if (job-util/failed-job-result? job-result)
          (throw (ex-info "Delete job failed" {:type ::delete-job-failed
                                               :result job-result}))
          (dsops/get-draftset-info backend draftset-ref))))

    silent?
    (dsops/get-draftset-info backend draftset-ref)

    :else
    (throw (ex-info "Graph not found" {:type ::graph-not-found :graph graph}))))

(defn remove-graph-from-draftset-handler
  "Ensures a user graph is deleted within a draftset"
  [{:keys [wrap-as-draftset-owner] manager :drafter/manager :as opts}]
  (let []
    (wrap-as-draftset-owner
     (parse-query-param-flag-handler
      :silent
      (feat-middleware/parse-graph-param-handler
       true
       (fn [{{:keys [draftset-id graph silent]} :params :as request}]
         (try
           (let [ds-info (delete-graph manager (req/user-id request) draftset-id graph silent)]
             (ring/response ds-info))
           (catch Exception exi
             (case (:type (ex-data exi))
               ::graph-not-found (drafter-response/unprocessable-entity-response (str "Graph not found"))
               ::delete-job-failed (let [job-result (:result (ex-data exi))]
                                     (async-responses/api-response 500 job-result))
               ;;unknown failure
               (throw exi))))))))))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.delete-by-graph/remove-graph-handler [_]
  (s/keys :req [:drafter/manager]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete-by-graph/remove-graph-handler [_ opts]
  (remove-graph-from-draftset-handler opts))
