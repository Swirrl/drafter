(ns drafter.routes.pages
  (:require [compojure.core :refer [GET routes]]
            [drafter.layout :as layout]
            [drafter.rdf.draft-management :refer [drafter-state-graph
                                                  live-graphs]]
            [drafter.rdf.drafter-ontology :refer [drafter]]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf :refer [add statements]]
            [grafter.rdf.formats :refer [rdf-trig]]
            [grafter.rdf.io :refer [rdf-serializer default-prefixes]]
            [ring.util.io :as rio]
            [ring.util.response :refer [not-found]]))

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
   (GET "/" [] (clojure.java.io/resource "swagger-ui/index.html"))

   (GET "/dump" []
        {:status 200
         :headers {"Content-Type" "text/plain; charset=utf-8"
                   "Content-Disposition" "inline;filename=drafter-state.trig"}
         :body (rio/piped-input-stream (partial dump-database db))})))
