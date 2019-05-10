(ns drafter.feature.draftset.options
  (:require [drafter.feature.middleware :as feat-middleware]
            [drafter.backend.draftset.operations :as dsops]
            [ring.util.response :as ring]
            [integrant.core :as ig]
            [clojure.spec.alpha :as s]))

(defn handler
  [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (wrap-authenticated
   (feat-middleware/existing-draftset-handler
    backend
    (fn [{{:keys [draftset-id]} :params user :identity}]
      (let [permitted (dsops/find-permitted-draftset-operations backend draftset-id user)]
        (ring/response permitted))))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-auth]))

(defmethod ig/init-key ::handler [_ opts]
  (handler opts))
