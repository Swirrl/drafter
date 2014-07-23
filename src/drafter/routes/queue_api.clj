(ns drafter.routes.queue-api
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [ring.util.io :as io]
            [ring.middleware.multipart-params]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]]
            [taoensso.timbre :as timbre]
            [drafter.rdf.queue :as q]
            [drafter.common.api-routes :as api-routes]
            [clojure.data.json :as json]))



(defn queue-api-routes [queue]
  (routes
   ; TODO: add parameters to allow filtering.
   (GET "/queue/peek" {}
      ; ditches id and job, returns id as the stringified job id.
      (let [queue-response (map (fn [j] (merge (dissoc j :id :job)
                                        {:id (str (:id j))})) (q/peek-jobs queue))]
        ; the middleware will turn it into json.
        (api-routes/api-response 200 {:queue queue-response})))))
