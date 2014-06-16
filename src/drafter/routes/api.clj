(ns drafter.routes.api
  (:import [com.hp.hpl.jena.tdb TDBFactory]
           [com.hp.hpl.jena.query QueryExecutionFactory QueryExecution Syntax Query])
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                    make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]

            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]]))

(def live (TDBFactory/assembleDataset "drafter-live.ttl"))

(def draft (TDBFactory/assembleDataset "drafter-draft.ttl"))


(defroutes sparql-routes
  (GET "/" [] "Hello World!!")
  (GET "/sparql" {params :query-params :as request}
       (pr-str request)))
