(ns drafter.routes.pages
  (:require [compojure.core :refer [GET routes context]]
            [clojure.java.io :as io]))

(defn pages-routes []
  (routes
   (GET "/" [] (io/resource "swagger-ui/index.html"))))
