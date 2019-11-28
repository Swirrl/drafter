(ns drafter.routes.status
  (:require [compojure.core :refer [GET routes]]))

(defn status-routes [{:keys [lock]}]
  (routes
   (GET "/writes-locked" [] (str (.isLocked lock)))))
