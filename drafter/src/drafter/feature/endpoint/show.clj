(ns drafter.feature.endpoint.show
  (:require [integrant.core :as ig]
            [drafter.feature.endpoint.public :as pub]
            [ring.util.response :as ring]))

(defn show-endpoint-handler [repo]
  (fn [_request]
    (if-let [endpoint (pub/get-public-endpoint repo)]
      (ring/response endpoint)
      (ring/not-found ""))))

(defmethod ig/init-key ::handler [_ {:keys [repo]}]
  (show-endpoint-handler repo))
