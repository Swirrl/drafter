(ns drafter.routes.status
  (:require [compojure.core :refer [GET routes]]
            [ring.util.io :as rio]
            [ring.util.response :refer [not-found]]
            [drafter.common.api-routes :as api])
  (:import [java.util UUID]))

(defn finished-job-route [job]
  (str "/status/finished-jobs/" (:id job)))

(defn status-routes [writes-lock finished-jobs]
  (routes
   (GET "/writes-locked" []
        (str (.isLocked writes-lock)))
   (GET "/finished-jobs/:job-id" [job-id]
        (let [p (get @finished-jobs (UUID/fromString job-id))]
          (if p
            @p
            (api/not-found-response "The specified job-id was not found"))))))
