(ns drafter.feature.draftset-data.delete-by-graph
  (:require
   [clojure.spec.alpha :as s]
   [drafter.async.jobs :as ajobs]
   [drafter.async.responses :as async-responses]
   [drafter.backend.draftset.draft-management :as mgmt]
   [drafter.backend.draftset.graphs :as graphs]
   [drafter.backend.draftset.operations :as dsops]
   [drafter.feature.common :as feat-common]
   [drafter.feature.middleware :as feat-middleware]
   [drafter.feature.modified-times :as modified-times]
   [drafter.job-responses :as job-response]
   [drafter.rdf.draftset-management.job-util :as jobs]
   [drafter.requests :as req]
   [drafter.responses :as response]
   [drafter.routes.draftsets-api :refer [parse-query-param-flag-handler]]
   [drafter.time :as time]
   [integrant.core :as ig]
   [ring.util.response :as ring]))

(defn- delete-graph-job [{:keys [backend graph-manager clock] :as manager} draftset-ref graph-uri]
  (let [draft-graph-uri (graphs/delete-user-graph graph-manager draftset-ref graph-uri)]
    (modified-times/draft-graph-deleted! backend graph-manager draftset-ref draft-graph-uri (time/now clock))
    nil))

(defn delete-graph
  "Deletes the given live graph within a draftset. Returns the draftset summary for the
   new draft state. The graph to delete must exist in the live graph unless silent? is
   false. An exception will be thrown if silent? is falsey and the graph does not exist,
   or if the delete operation fails for some reason. In these cases the ex-info of the
   exception will be a map containing a :type key indicating the reason for the error."
  [{:keys [backend] :as manager} user-id draftset-ref graph silent?]
  (cond
    (mgmt/is-graph-managed? backend graph)
    (let [job-result (feat-common/run-sync
                      manager
                      user-id
                      'delete-draftset-graph
                      draftset-ref
                      #(delete-graph-job manager draftset-ref graph)
                      identity)]
      (if (jobs/failed-job-result? job-result)
        (throw (ex-info "Delete job failed" {:type ::delete-job-failed
                                             :result job-result}))
        (dsops/get-draftset-info backend draftset-ref)))

    silent?
    (dsops/get-draftset-info backend draftset-ref)

    :else
    (throw (ex-info "Graph not found" {:type ::graph-not-found :graph graph}))))

(defn sync-job [{:keys [drafter/manager]}]
  (fn [draftset-id graph user-id silent]
    (try
      (let [ds-info (delete-graph manager user-id draftset-id graph silent)]
        (ring/response ds-info))
      (catch Exception exi
        (case (:type (ex-data exi))
          ::graph-not-found (response/unprocessable-entity-response
                             "Graph not found")
          ::delete-job-failed (let [job-result (:result (ex-data exi))]
                                (async-responses/api-response 500 job-result))
          ;;unknown failure
          (throw exi))))))

(defn async-job [{:keys [drafter/manager]}]
  (fn [draftset-id graph user-id silent metadata]
    (let [response #(job-response/submit-async-job!
                      (jobs/make-job user-id :background-write
                        (jobs/job-metadata
                          (:backend manager) draftset-id 'delete-draftset-graph metadata)
                        (fn [job] (ajobs/job-succeeded! job (%)))))]
      (if (mgmt/is-graph-managed? (:backend manager) graph)
        (response #(graphs/delete-user-graph (:graph-manager manager)
                                             draftset-id
                                             graph))
        (if silent
          (response #(dsops/get-draftset-info (:backend manager) draftset-id))
          (response/unprocessable-entity-response "Graph not found"))))))

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
    :req [:drafter/manager]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete-by-graph/sync-job-handler
  [_ opts]
  (sync-job opts))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.delete-by-graph/async-job-handler [_]
  (s/keys
    :req [:drafter/manager]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete-by-graph/async-job-handler
  [_ opts]
  (async-job opts))

(s/def ::sync-job-handler fn?)
(s/def ::async-job-handler fn?)

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.delete-by-graph/remove-graph-handler [_]
  (s/keys :req [:drafter/manager]
          :req-un [::wrap-as-draftset-owner ::sync-job-handler ::async-job-handler]))

(defmethod ig/init-key :drafter.feature.draftset-data.delete-by-graph/remove-graph-handler [_ opts]
  (remove-graph-from-draftset-handler opts))
