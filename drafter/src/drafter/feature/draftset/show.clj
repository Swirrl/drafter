(ns drafter.feature.draftset.show
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.endpoint :as ep]
            [drafter.feature.endpoint.public :as pub]
            [drafter.feature.middleware :as feat-middleware]
            [drafter.middleware :as middleware]
            [drafter.responses :refer [forbidden-response]]
            [drafter.routes.draftsets-api :refer [parse-union-with-live-handler]]
            [drafter.user :as user]
            [integrant.core :as ig]
            [ring.util.response :as ring]))

(defn get-draftset [backend user draftset-id union-with-live?]
  (if-let [info (dsops/get-draftset-info backend draftset-id)]
    (if (user/can-view? user info)
      (if union-with-live?
        (let [public-endpoint (pub/get-public-endpoint backend)]
          (ep/merge-endpoints public-endpoint info))
        info)
      ::inaccessible)
    ::not-found))

(defn handler
  [{:keys [drafter/backend wrap-authenticate]}]
  (middleware/wrap-authorize wrap-authenticate :drafter:draft:view
   (feat-middleware/existing-draftset-handler
    backend
    (parse-union-with-live-handler
      (fn [{{:keys [draftset-id union-with-live]} :params user :identity :as request}]
        (let [ds-info-or-reason (get-draftset backend user draftset-id union-with-live)]
          (case ds-info-or-reason
            ::not-found (ring/not-found "")
            ::inaccessible (forbidden-response "Draftset not in accessible state")
            (ring/response ds-info-or-reason))))))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend]))

(defmethod ig/init-key ::handler [_ opts]
  (handler opts))
