(ns drafter.feature.draftset-data.append-by-graph
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as ops]
            [drafter.feature.middleware :as middleware]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.responses :as response :refer [submit-async-job!]]
            [drafter.util :as util]
            [integrant.core :as ig]
            [drafter.feature.draftset-data.common :as ds-data-common]
            [drafter.draftset :as ds]
            [drafter.requests :as req]))

(defn create-or-empty-draft-graph-for [repo draftset-ref live-graph clock-fn]
  (if-let [draft-graph-uri (ops/find-draftset-draft-graph repo draftset-ref live-graph)]
    (do
      (mgmt/delete-graph-contents! repo draft-graph-uri (clock-fn))
      draft-graph-uri)
    (mgmt/create-draft-graph! repo live-graph draftset-ref clock-fn)))

(defn copy-live-graph-into-draftset-job
  [resources user-id draftset-ref live-graph]
  (let [ds-id (ds/->draftset-id draftset-ref)
        repo (-> resources :backend :repo)]
    (jobs/make-job repo user-id 'copy-live-graph-into-draftset ds-id :background-write
      (fn [job]
        (let [draft-graph-uri (create-or-empty-draft-graph-for repo draftset-ref live-graph util/get-current-time)]
          (ds-data-common/lock-writes-and-copy-graph resources live-graph draft-graph-uri {:silent true})
          (jobs/job-succeeded! job))))))

(defn- required-live-graph-param-handler [repo inner-handler]
  (fn [{{:keys [graph]} :params :as request}]
    (if (mgmt/is-graph-live? repo graph)
      (inner-handler request)
      (response/unprocessable-entity-response (str "Graph not found in live")))))

(defn put-draftset-graph-handler
  [{:keys [:drafter/backend :drafter/global-writes-lock wrap-as-draftset-owner]}]
  (letfn [(required-live-graph-param [handler]
            (middleware/parse-graph-param-handler
             true
             (required-live-graph-param-handler (:repo backend) handler)))]
    (wrap-as-draftset-owner
     (required-live-graph-param
      (fn [{{:keys [draftset-id graph]} :params :as request}]
        (-> {:backend backend :global-writes-lock global-writes-lock}
            (copy-live-graph-into-draftset-job (req/user-id request)
                                               draftset-id
                                               graph)
            (submit-async-job!)))))))

(defmethod ig/pre-init-spec :drafter.feature.draftset-data.append-by-graph/handler [_]
  (s/keys :req [:drafter/backend :drafter/global-writes-lock]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset-data.append-by-graph/handler [_ opts]
  (put-draftset-graph-handler opts))
