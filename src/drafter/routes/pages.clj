(ns drafter.routes.pages
  (:use compojure.core)
  (:require [drafter.layout :as layout]
            [drafter.util :as util]))

(defn query-page [params]
  (layout/render "query.html" params))

(defroutes pages-routes
  (GET "/" [] (ring.util.response/redirect "/live"))
  (GET "/live" [] (query-page {:endpoint "/sparql/live"
                               :update-endpoint "/sparql/live/update"
                               :name "Live" }))
  (GET "/draft" [] (query-page {:endpoint "/sparql/draft"
                                :update-endpoint "/sparql/draft/update"
                                :name "Draft" }))
  (GET "/state" [] (query-page {:endpoint "/sparql/state"
                                :update-endpoint "/sparql/state/update"
                                :name "State" })))
