(ns drafter.routes.status
  (:require [compojure.core :refer [GET routes]]
            [swirrl-server.async.status-routes :as st]))

(defn status-routes [writes-lock finished-jobs restart-id]
  (routes
   (GET "/writes-locked" []
        (str (.isLocked writes-lock)))
   (st/status-routes finished-jobs restart-id)))
