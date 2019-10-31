(ns drafter.feature.draftset.set-metadata
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [integrant.core :as ig]
            [drafter.requests :as req]))

(defn handler
  [{backend :drafter/backend :keys [wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (fn [{{:keys [draftset-id] :as params} :params :as request}]
     (feat-common/run-sync
      backend
      (req/user-id request)
      'set-draftset-metadata
      draftset-id
      #(dsops/set-draftset-metadata! backend draftset-id params)
      #(feat-common/draftset-sync-write-response % backend draftset-id)))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::handler [_ opts]
  (handler opts))
