(ns drafter.feature.draftset.show
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.feature.middleware :as feat-middleware]
            [drafter.feature.endpoint.public :as pub]
            [drafter.routes.draftsets-api :refer [parse-union-with-live-handler]]
            [drafter.responses :refer [forbidden-response]]
            [drafter.endpoint :as ep]
            [drafter.user :as user]
            [integrant.core :as ig]
            [ring.util.response :as ring]))

(defn handler
  [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (wrap-authenticated
   (feat-middleware/existing-draftset-handler
    backend
    (parse-union-with-live-handler
      (fn [{{:keys [draftset-id union-with-live]} :params user :identity :as request}]
        (if-let [info (dsops/get-draftset-info backend draftset-id)]
          (if (user/can-view? user info)
            (if union-with-live
              (let [public-endpoint (pub/get-public-endpoint backend)
                    info (ep/merge-endpoints public-endpoint info)]
                (ring/response info))
              (ring/response info))
            (forbidden-response "Draftset not in accessible state"))
          (ring/not-found "")))))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-auth]))

(defmethod ig/init-key ::handler [_ opts]
  (handler opts))
