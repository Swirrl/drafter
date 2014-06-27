(ns drafter.routes.home
  (:use compojure.core)
  (:require [drafter.layout :as layout]
            [drafter.util :as util]))

(defn home-page []
  (layout/render
    "home.html" {:content (util/md->html "/md/docs.md")}))

(defn query-page []
  (layout/render "query.html"))

(defroutes home-routes
  (GET "/" [] (home-page))
  (GET "/query" [] (query-page)))
