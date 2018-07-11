(ns drafter.routes.dump
  (:require [clojure.tools.logging :as log]
            [compojure.core :refer [context GET routes]]
            [drafter.backend.draftset.draft-management :refer [drafter-state-graph]]
            [drafter.rdf.drafter-ontology :refer [drafter]]
            [grafter.rdf :refer [add statements]]
            [grafter.rdf4j.io :as gio :refer [default-prefixes rdf-writer]]
            [grafter.rdf4j.repository :as repo]
            [grafter.url :as url]
            [ring.util.io :as rio]
            [ring.util.response :refer [not-found]]))

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
