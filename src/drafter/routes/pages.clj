(ns drafter.routes.pages
  (:require [compojure.core :refer [GET routes]]
            [drafter.layout :as layout]
            [grafter.rdf.io :refer [default-prefixes]]
            [drafter.rdf.draft-management :refer [drafter-state-graph
                                                  live-graphs]]
            [drafter.backend.protocols :refer [get-all-drafts get-live-graph-for-draft]]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf :refer [add statements]]
            [grafter.rdf.formats :refer [rdf-trig]]
            [grafter.rdf.io :refer [rdf-serializer]]
            [ring.util.io :as rio]
            [ring.util.response :refer [not-found]]))

(defn query-page [params]
  (layout/render "query-page.html" params))

(defn draft-management-page [params]
  (layout/render "draft/draft-management.html" params))

(defn upload-form [params]
  (layout/render "upload.html" params ))

(def drafter-prefixes (merge default-prefixes {"draft" "http://publishmydata.com/graphs/drafter/draft/"
                                               "drafter" "http://publishmydata.com/def/drafter/"
                                               "folder" "http://publishmydata.com/def/ontology/folder/"}))

(defn dump-database
  "A convenience function intended for development use.  It will dump
  the RAW database as a Trig String for debugging.  Don't use on large
  databases as it will be loaded into memory."
  [db ostream]
  (add (rdf-serializer ostream
                       :format rdf-trig
                       :prefixes drafter-prefixes
                       ) (statements db)))

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
   (GET "/draft" []
        (draft-management-page {:endpoint "/sparql/draft"
                                :update-endpoint "/sparql/draft/update"
                                :name "Draft"
                                :drafts (get-all-drafts db)
                                :dump-path "/draft/data"
                                }))

   (GET "/live/data" request
        (live-dumps-form "/data/live" (doall (live-graphs db))))

   (GET "/draft/data" request
        (draft-dumps-form "/data/draft" (get-all-drafts db)))

   (GET "/draft/:guid" [guid]
        (let [draft (draft-uri guid)]
          (if-let [live-uri (get-live-graph-for-draft db draft)]
            (upload-form {:draft draft :live live-uri})
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
