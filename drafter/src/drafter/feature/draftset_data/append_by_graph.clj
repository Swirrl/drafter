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
   [drafter.responses :as response :refer [submit-async-job!]]
   [drafter.time :as time]
   [integrant.core :as ig]))

(defn create-or-empty-draft-graph-for [repo graph-manager draftset-ref live-graph clock]
  (if-let [draft-graph-uri (ops/find-draftset-draft-graph repo draftset-ref live-graph)]
    (do
      (mgmt/delete-graph-contents! repo draft-graph-uri (time/now clock))
      draft-graph-uri)
    (graphs/create-user-graph-draft graph-manager draftset-ref live-graph)))

(defn copy-live-graph-into-draftset-job [resources user-id {:keys [draftset-id graph metadata]} clock]
  (let [repo (-> resources :backend :repo)
        graph-manager (:graph-manager resources)]
    (jobs/make-job user-id
                   :background-write
                   (jobs/job-metadata repo draftset-id 'copy-live-graph-into-draftset metadata)
                   (fn [job]
                     (let [draft-graph-uri (create-or-empty-draft-graph-for repo graph-manager draftset-id graph clock)]
                       (ds-data-common/lock-writes-and-copy-graph resources graph draft-graph-uri {:silent true})
                       (mgmt/rewrite-draftset! repo
                                               {:draftset-uri (ds/->draftset-uri draftset-id)
                                               ; :live-graph-uris [graph]
                                                })
                       (ajobs/job-succeeded! job))))))

(defn- required-live-graph-param-handler [repo inner-handler]
  (fn [{{:keys [graph]} :params :as request}]
    (if (mgmt/is-graph-live? repo graph)
      (inner-handler request)
      (response/unprocessable-entity-response (str "Graph not found in live")))))

(defn put-draftset-graph-handler
  [{:keys [:drafter/backend :drafter.backend.draftset.graphs/manager :drafter/global-writes-lock wrap-as-draftset-owner
           ::time/clock]}]
  (letfn [(required-live-graph-param [handler]
            (middleware/parse-graph-param-handler
             true
             (required-live-graph-param-handler (:repo backend) handler)))]
    (wrap-as-draftset-owner
     (required-live-graph-param
      (fn [{:keys [params] :as request}]
        (let [resources {:backend backend
                         :global-writes-lock global-writes-lock
                         :graph-manager manager}
              job (copy-live-graph-into-draftset-job resources (req/user-id request) params clock)]
          (submit-async-job! job)))))))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.append-by-graph/handler [_]
  (s/keys :req [:drafter/backend :drafter/global-writes-lock]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset-data.append-by-graph/handler [_ opts]
  (put-draftset-graph-handler opts))
