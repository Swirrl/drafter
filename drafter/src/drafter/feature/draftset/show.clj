(ns drafter.feature.draftset.show
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.middleware :as feat-middleware]
            [drafter.responses :refer [forbidden-response]]
            [drafter.user :as user]
            [integrant.core :as ig]
            [ring.util.response :as ring]))

(defn handler
  [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (wrap-authenticated
   (feat-middleware/existing-draftset-handler
    backend
    (fn [{{:keys [draftset-id]} :params user :identity :as request}]
      (if-let [info (dsops/get-draftset-info backend draftset-id)]
        (if (user/can-view? user info)
          (ring/response info)
          (forbidden-response "Draftset not in accessible state"))
        (ring/not-found ""))))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-auth]))

(defmethod ig/init-key ::handler [_ opts]
  (handler opts))
