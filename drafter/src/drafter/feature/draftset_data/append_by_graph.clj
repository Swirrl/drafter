(ns drafter.feature.draftset-data.append-by-graph
  (:require
   [clojure.spec.alpha :as s]
   [drafter.async.jobs :as ajobs]
   [drafter.backend.draftset.draft-management :as mgmt]
   [drafter.backend.draftset.graphs :as graphs]
   [drafter.backend.draftset.operations :as ops]
   [drafter.draftset :as ds]
   [drafter.feature.draftset-data.common :as ds-data-common]
   [drafter.feature.middleware :as middleware]
   [drafter.rdf.draftset-management.job-util :as jobs]
   [drafter.requests :as req]
   [drafter.responses :as response]
   [drafter.write-scheduler :as writes]
   [integrant.core :as ig]))

(defn create-or-empty-draft-graph-for [repo graph-manager draftset-ref live-graph]
  (if-let [draft-graph-uri (ops/find-draftset-draft-graph repo draftset-ref live-graph)]
    (do
      (mgmt/delete-graph-contents! repo draft-graph-uri)
      draft-graph-uri)
    (graphs/create-user-graph-draft graph-manager draftset-ref live-graph)))

(defn copy-live-graph-into-draftset-job [{:keys [backend graph-manager] :as manager} user-id draftset-id graph metadata]
  (jobs/make-job user-id
                 :background-write
                 (jobs/job-metadata backend draftset-id 'copy-live-graph-into-draftset metadata)
                 (fn [job]
                   (let [draft-graph-uri (create-or-empty-draft-graph-for backend graph-manager draftset-id graph)]
                     (ds-data-common/lock-writes-and-copy-graph manager graph draft-graph-uri {:silent true})
                     (mgmt/rewrite-draftset! backend
                                             {:draftset-uri (ds/->draftset-uri draftset-id)
                                              ; :live-graph-uris [graph]
                                              })
                     (ajobs/job-succeeded! job)))))

(defn- required-live-graph-param-handler [repo inner-handler]
  (fn [{{:keys [graph]} :params :as request}]
    (if (mgmt/is-graph-live? repo graph)
      (inner-handler request)
      (response/unprocessable-entity-response (str "Graph not found in live")))))

(defn copy-live-graph
  "Enqueues a job to copy graph into draftest on behalf of the given user"
  [manager draftset user-id graph metadata]
  (let [job (copy-live-graph-into-draftset-job manager user-id draftset graph metadata)]
    (writes/enqueue-async-job! job)))

(defn put-draftset-graph-handler
  [{:keys [wrap-as-draftset-owner] {:keys [backend] :as manager} :drafter/manager}]
  (letfn [(required-live-graph-param [handler]
            (middleware/parse-graph-param-handler
             true
             (required-live-graph-param-handler backend handler)))]
    (wrap-as-draftset-owner :draft:edit
     (required-live-graph-param
      (fn [{:keys [params] :as request}]
        (let [{:keys [draftset-id graph metadata]} params
              job (copy-live-graph manager draftset-id (req/user-id request) graph metadata)]
          (response/submitted-job-response job)))))))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.append-by-graph/handler [_]
  (s/keys :req [:drafter/manager] :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset-data.append-by-graph/handler [_ opts]
  (put-draftset-graph-handler opts))
