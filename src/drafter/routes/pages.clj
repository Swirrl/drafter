(ns drafter.routes.pages
  (:require [compojure.core :refer [GET routes context]]
            [drafter.layout :as layout]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf :refer [add statements]]
            [grafter.rdf
             [formats :refer [rdf-trig]]
             [io :refer [default-prefixes rdf-serializer]]]
            [ring.util.io :as rio]))

(def drafter-prefixes (assoc default-prefixes
                             "drafter" (drafter "")
                             "draftset" (drafter "draftset/")
                             "graph" "http://publishmydata.com/graphs/drafter/draft/"))

(defn query-page [params]
  (layout/render "query-page.html" params))

(defn dump-database
  "A convenience function intended for development use.  It will dump
  the RAW database as a Trig String for debugging.  Don't use on large
  databases as it will be loaded into memory."
  [db ostream]
  (add (rdf-serializer ostream :format rdf-trig :prefixes drafter-prefixes)
       (statements db)))

(defn pages-routes [db]
  (routes
   (GET "/" [] (clojure.java.io/resource "swagger-ui/index.html"))))
