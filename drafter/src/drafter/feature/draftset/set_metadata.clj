(ns drafter.feature.draftset.set-metadata
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [integrant.core :as ig]
            [drafter.requests :as req]))

(defn handler
  [{:keys [:drafter/backend :drafter/global-writes-lock wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (fn [{{:keys [draftset-id] :as params} :params :as request}]
     (feat-common/run-sync
      {:backend backend :global-writes-lock global-writes-lock}
      (req/user-id request)
      'set-draftset-metadata
      draftset-id
      #(dsops/set-draftset-metadata! backend draftset-id params)
      #(feat-common/draftset-sync-write-response % backend draftset-id)))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.set-metadata/handler [_]
  (s/keys :req [:drafter/backend :drafter/global-writes-lock]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset.set-metadata/handler [_ opts]
  (handler opts))
