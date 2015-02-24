(ns drafter.layout
  (:require [clojure.string :as s]
            [compojure.response :refer [Renderable]]
            [ring.util.response :refer [content-type response]]
            [selmer.parser :as parser]))

(def template-path "templates/")

(deftype RenderableTemplate [template params]
  Renderable
  (render [this request]
    (content-type
      (->> (assoc params
                  (keyword (s/replace template #".html" "-selected")) "active"
                  :servlet-context (:context request))
        (parser/render-file (str template-path template))
        response)
      "text/html; charset=utf-8")))

(defn render [template & [params]]
  (RenderableTemplate. template params))

