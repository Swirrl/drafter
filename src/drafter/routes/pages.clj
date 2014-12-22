(ns drafter.routes.pages
  (:require [compojure.core :refer [GET routes]]
            [clojure.walk :refer [keywordize-keys]]
            [drafter.layout :as layout]
            [ring.util.io :as rio]
            [ring.util.response :refer [not-found]]
            [drafter.util :as util]
            [grafter.rdf :refer [add statements]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.draft-management :refer [drafter-state-graph lookup-live-graph live-graphs]]
            [grafter.rdf.formats :refer [rdf-trig]]
            [grafter.rdf.io :refer [rdf-serializer]]
            [grafter.rdf.repository :refer [query ->connection]]
            [clojure.tools.logging :as log])
  (:import [org.openrdf.repository RepositoryConnection]))

(defn query-page [params]
  (layout/render "query-page.html" params))

(defn draft-management-page [params]
  (layout/render "draft/draft-management.html" params))

(defn upload-form [params]
  (layout/render "upload.html" params ))

(defn dump-database
  "A convenience function intended for development use.  It will dump
  the RAW database as a Trig String for debugging.  Don't use on large
  databases as it will be loaded into memory."
  [db ostream]
  (add (rdf-serializer ostream :format rdf-trig) (statements db)))

(defn parse-guid [uri]
  (.replace (str uri) (draft-uri "") ""))

(defn map-values [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn all-drafts [db]
  (doall (->> (query db (str
                         "SELECT ?draft ?live WHERE {"
                         "   GRAPH <" drafter-state-graph "> {"
                         "     ?draft a <" drafter:DraftGraph "> . "
                         "     ?live <" drafter:hasDraft "> ?draft . "
                         "   }"
                         "}"))
              (map keywordize-keys)
              (map (partial map-values str))
              (map (fn [m] (assoc m :guid (parse-guid (:draft m))))))))

(defn draft-exists? [db guid]
  (query db
         (str "ASK {"
              "  GRAPH <" drafter-state-graph "> {"
              "    <" (draft-uri guid) "> a <" drafter:DraftGraph "> ."
              "  }"
              "}")))

(defn data-page [template dumps-endpoint graphs]
  (layout/render template {:dump-path dumps-endpoint :graphs graphs}))

(def live-dumps-form (partial data-page "dumps-page.html"))

(def draft-dumps-form (partial data-page "draft/dumps-page.html"))

(defn pages-routes [db]
  (routes
   (GET "/" [] (ring.util.response/redirect "/live"))
   (GET "/live" [] (query-page {:endpoint "/sparql/live"
                                :update-endpoint "/sparql/live/update"
                                :dump-path "/live/data"
                                :name "Live" }))
   (GET "/draft" [] (with-open [conn (->connection db)]
                      (draft-management-page {:endpoint "/sparql/draft"
                                              :update-endpoint "/sparql/draft/update"
                                              :name "Draft"
                                              :drafts (all-drafts conn)
                                              :dump-path "/draft/data"
                                              })))

   (GET "/live/data" request
        (live-dumps-form "/data/live" (doall (live-graphs db))))

   (GET "/draft/data" request
        (draft-dumps-form "/data/draft" (doall (all-drafts db))))

   (GET "/draft/:guid" [guid]
        (with-open [conn (->connection db)]
          (if (draft-exists? conn guid)
            (let [draft (draft-uri guid)
                  live (lookup-live-graph conn draft)]
              (upload-form {:draft draft :live live}))
            (not-found (str "No such Draft:" guid)))))

   (GET "/state" [] (query-page {:endpoint "/sparql/state"
                                 :update-endpoint "/sparql/state/update"
                                 :name "State" }))

   (GET "/raw" [] (query-page {:endpoint "/sparql/raw"
                               :update-endpoint "/sparql/raw/update"
                               :name "Raw" }))

   (GET "/dump" []
        {:status 200
         :headers {"Content-Type" "text/plain; charset=utf-8"
                   "Content-Disposition" "inline;filename=drafter-state.trig"}
         :body (rio/piped-input-stream (partial dump-database db))})))
