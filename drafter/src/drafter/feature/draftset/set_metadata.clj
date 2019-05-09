(ns drafter.feature.draftset.set-metadata
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.common :as feat-common]
            [integrant.core :as ig]))

(defn handler
  [{backend :drafter/backend :keys [wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (fn [{{:keys [draftset-id] :as params} :params}]
     (feat-common/run-sync #(dsops/set-draftset-metadata! backend draftset-id params)
               #(feat-common/draftset-sync-write-response % backend draftset-id)))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::handler [_ opts]
  (handler opts))
