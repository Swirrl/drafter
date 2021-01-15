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
            [drafter.requests :as req]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.responses :as response]
            [drafter.async.jobs :as ajobs]
            [clojure.set :as set]))

(defn sync-job [{:keys [backend] :as resources}]
  (fn [draftset-id graph user-id silent]
    (if (mgmt/is-graph-managed? backend graph)
      (feat-common/run-sync
        resources
        user-id
        'delete-draftset-graph
        draftset-id
        #(dsops/delete-draftset-graph! backend draftset-id graph util/get-current-time)
        #(feat-common/draftset-sync-write-response % backend draftset-id))
      (if silent
        (ring/response (dsops/get-draftset-info backend draftset-id))
        (drafter-response/unprocessable-entity-response (str "Graph not found"))))))

(defn async-job [{:keys [backend] :as resources}]
  (fn [draftset-id graph user-id silent metadata]
    (if (mgmt/is-graph-managed? backend graph)
      (-> (jobs/make-job user-id :background-write
            (jobs/job-metadata backend draftset-id
              'delete-draftset-graph metadata)
            (fn [job]
              (let [result (dsops/delete-draftset-graph! backend
                             draftset-id
                             graph
                             util/get-current-time)]
                (ajobs/job-succeeded! job result))))
        (response/submit-async-job!))
      (if silent
        (-> (jobs/make-job user-id :background-write
              (jobs/job-metadata backend draftset-id
                'delete-draftset-graph metadata)
              (fn [job]
                (let [result (dsops/get-draftset-info backend draftset-id)]
                  (ajobs/job-succeeded! job result))))
          (response/submit-async-job!))
        (drafter-response/unprocessable-entity-response
          (str "Graph not found"))))))

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
  (s/keys :req-un [::backend ::global-writes-lock]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete-by-graph/sync-job-handler
  [_ {:keys [backend] :as resources}]
  (sync-job resources))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.delete-by-graph/async-job-handler [_]
  (s/keys :req-un [::backend]))

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
