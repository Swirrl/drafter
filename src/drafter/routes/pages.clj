(ns drafter.routes.pages
  (:require [compojure.core :refer [GET routes]]
            [drafter.pages.query-page :refer [query-page all-drafts]]
            [drafter.layout :as layout]
            [net.cgrand.enlive-html :as en]
            [ring.util.io :as rio]
            [ring.util.response :refer [not-found]]
            [drafter.util :as util]
            [grafter.rdf :refer [add statements]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.draft-management :refer [drafter-state-graph lookup-live-graph live-graphs]]
            [grafter.rdf.formats :refer [rdf-trig]]
            [grafter.rdf.io :refer [rdf-serializer]]
            [grafter.rdf.repository :refer [query ->connection]]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]])
  (:import [org.openrdf.repository RepositoryConnection]))

(en/deftemplate layout "dist/html/drafter.html"
  [{:keys [site-name site-url]} body]
  [:section.masthead :a]
  (en/do-> (en/set-attr :href site-url)
           (en/content site-name))
  [:main :#contents :section]
  (en/substitute body))

(defn render [nodes]
  (apply str nodes))

(defn layout-params [env]
  (let [site-name (or (:drafter-site-name env)
                      "drafter-site-name")
        site-url (or (:drafter-site-url env)
                     "#")]
    {:site-name site-name :site-url site-url}))

(defn apply-layout [params content]
  (render (layout (layout-params params) content)))

(defn draft-management-page [params]
  ;;(layout/render "draft/draft-management.html" params)
  )

(defn upload-form [params]
  ;;(layout/render "upload.html" params )
  )

(defn dump-database
  "A convenience function intended for development use.  It will dump
  the RAW database as a Trig String for debugging.  Don't use on large
  databases as it will be loaded into memory."
  [db ostream]
  (add (rdf-serializer ostream :format rdf-trig) (statements db)))

(defn draft-exists? [db guid]
  (query db
         (str "ASK {"
              "  GRAPH <" drafter-state-graph "> {"
              "    <" (draft-uri guid) "> a <" drafter:DraftGraph "> ."
              "  }"
              "}")))

(defn data-page [template dumps-endpoint graphs]
  ;(layout/render template {:dump-path dumps-endpoint :graphs graphs})
  )

(def live-dumps-form (partial data-page "dumps-page.html"))

(def draft-dumps-form (partial data-page "draft/dumps-page.html"))

(defn pages-routes [db]
  (routes
   (GET "/" [] (ring.util.response/redirect "/live"))
   (GET "/live" [] (apply-layout (merge env {:name "Live"})
                                 (query-page "Live endpoint" {:action-uri "/sparql/live"
                                                              :graph-uris []})))

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
