(ns drafter.feature.draftset-data.delete-by-graph
  (:require
   [clojure.spec.alpha :as s]
   [drafter.async.jobs :as ajobs]
   [drafter.backend.draftset.draft-management :as mgmt]
   [drafter.backend.draftset.graphs :as graphs]
   [drafter.backend.draftset.operations :as dsops]
   [drafter.feature.common :as feat-common]
   [drafter.feature.middleware :as feat-middleware]
   [drafter.rdf.draftset-management.job-util :as jobs]
   [drafter.requests :as req]
   [drafter.responses :as response]
   [drafter.routes.draftsets-api :refer [parse-query-param-flag-handler]]
   [integrant.core :as ig]
   [ring.util.response :as ring]))

(defn sync-job [{:keys [backend] graph-manager ::graphs/manager :as resources}]
  (fn [draftset-id graph user-id silent]
    (if (mgmt/is-graph-managed? backend graph)
      (feat-common/run-sync
        resources
        user-id
        'delete-draftset-graph
        draftset-id
        #(graphs/delete-user-graph graph-manager draftset-id graph)
        #(feat-common/draftset-sync-write-response % backend draftset-id))
      (if silent
        (ring/response (dsops/get-draftset-info backend draftset-id))
        (response/unprocessable-entity-response (str "Graph not found"))))))

(defn async-job [{:keys [backend] graph-manager ::graphs/manager}]
  (fn [draftset-id graph user-id silent metadata]
    (let [response #(response/submit-async-job!
                      (jobs/make-job user-id :background-write
                        (jobs/job-metadata
                          backend draftset-id 'delete-draftset-graph metadata)
                        (fn [job] (ajobs/job-succeeded! job (%)))))]
      (if (mgmt/is-graph-managed? backend graph)
        (response #(graphs/delete-user-graph graph-manager draftset-id graph))
        (if silent
          (response #(dsops/get-draftset-info backend draftset-id))
          (response/unprocessable-entity-response
            (str "Graph not found")))))))

(defn request-handler
  [{:keys [:drafter/backend sync-job-handler async-job-handler] :as _resources}]
  (fn [{{:keys [draftset-id graph silent metadata]} :params
       {:strs [perform-async] :or {perform-async "false"}} :headers
       :as request}]
    (if (Boolean/parseBoolean perform-async)
      (async-job-handler draftset-id graph (req/user-id request) silent metadata)
      (sync-job-handler  draftset-id graph (req/user-id request) silent))))

(defn remove-graph-from-draftset-handler
  "Remove a supplied graph from the draftset."
  [{:keys [wrap-as-draftset-owner] :as resources}]
  (wrap-as-draftset-owner
   (parse-query-param-flag-handler
    :silent
    (feat-middleware/parse-graph-param-handler true (request-handler resources)))))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.delete-by-graph/sync-job-handler [_]
  (s/keys
    :req [::graphs/manager]
    :req-un [::backend ::global-writes-lock]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete-by-graph/sync-job-handler
  [_ {:keys [backend] :as resources}]
  (sync-job resources))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.delete-by-graph/async-job-handler [_]
  (s/keys
    :req [::graphs/manager]
    :req-un [::backend]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete-by-graph/async-job-handler
  [_ {:keys [backend] :as resources}]
  (async-job resources))

(s/def ::sync-job-handler fn?)
(s/def ::async-job-handler fn?)

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.delete-by-graph/remove-graph-handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner ::sync-job-handler ::async-job-handler]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete-by-graph/remove-graph-handler [_ opts]
  (remove-graph-from-draftset-handler opts))
