(ns drafter.core
  (:require [compojure.core :refer [context defroutes routes routing let-request
                                   make-route let-routes
                                    ANY GET POST PUT DELETE HEAD]]
            [compojure.route :refer [not-found]]
            [compojure.handler :refer [api]]))

(defroutes drafter-routes
  (GET "/" [] "Hello World!!")
  (GET "/sparql" {params :query-params :as request}
       (pr-str request))
  (ANY "/*" [] (not-found "Not found")))

(def drafter-app (-> drafter-routes api))
