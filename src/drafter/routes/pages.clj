(ns drafter.routes.pages
  (:use compojure.core)
  (:require [drafter.layout :as layout]
            [drafter.util :as util]))

(defn query-page [params]
  (layout/render "query.html" params))

(defroutes pages-routes
  (GET "/" [] (ring.util.response/redirect "/live"))
  (GET "/live" [] (query-page { :endpoint "/sparql/live" :name "Live" }))
  (GET "/draft" [] (query-page { :endpoint "/sparql/draft" :name "Draft" }))
  (GET "/state" [] (query-page { :endpoint "/sparql/state" :name "State" })))
