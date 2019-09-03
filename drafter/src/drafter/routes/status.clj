(ns drafter.routes.status
  (:require [compojure.core :refer [GET routes]]))

(defn status-routes [writes-lock]
  (routes
   (GET "/writes-locked" []
        (str (.isLocked writes-lock)))))
