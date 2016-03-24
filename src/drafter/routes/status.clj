(ns drafter.routes.status
  (:require [compojure.core :refer [GET routes]]
            [ring.util.response :refer [not-found response]]
            [swirrl-server.async.status-routes :as st])
  (:import [java.util UUID]))

(defn status-routes [writes-lock finished-jobs restart-id]
  (routes
   (GET "/writes-locked" []
        (str (.isLocked writes-lock)))
   (st/status-routes finished-jobs restart-id)))
