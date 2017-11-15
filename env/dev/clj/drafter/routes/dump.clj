(ns drafter.routes.dump
  (:require [compojure.core :refer [GET routes context]]
            [drafter.rdf.draft-management :refer [drafter-state-graph]]
            [drafter.rdf.drafter-ontology :refer [drafter]]
            [grafter.rdf :refer [add statements]]
            [grafter.rdf4j.io :refer [default-prefixes rdf-writer] :as gio]
            [grafter.rdf4j.repository :as repo]
            [ring.util.io :as rio]
            [ring.util.response :refer [not-found]]
            [grafter.url :as url]
            [clojure.tools.logging :as log]))

(def drafter-prefixes (assoc default-prefixes
                             "drafter" drafter
                             "draftset" (url/append-path-segments drafter "draftset/")
                             "graph" (url/->grafter-url "http://publishmydata.com/graphs/drafter/draft/")))

(defn dump-database
  "A convenience function intended for development use.  It will dump
  the RAW database as a Trig String for debugging.  Don't use on large
  databases as it will be loaded into memory."
  [db ostream]

  (with-open [conn (repo/->connection db)]
    (add (rdf-writer ostream :format :trig :prefixes drafter-prefixes)
         (statements conn))))

(defn build-dump-route [backend]
  (GET "/dump" []
       {:status 200
        :headers {"Content-Type" "text/plain; charset=utf-8"
                  "Content-Disposition" "inline;filename=drafter-state.trig"}
        :body (rio/piped-input-stream (partial dump-database backend))}))
