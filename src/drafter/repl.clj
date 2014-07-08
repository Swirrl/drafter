(ns drafter.repl
  (:use
        [ring.middleware file-info file])
  (:require [grafter.rdf.protocols :refer [add add-statement statements]]
            [grafter.rdf.sesame :refer [query prepare-query evaluate with-transaction]]
            [drafter.rdf.queue :as q]
            [drafter.rdf.draft-management :refer :all]

            [drafter.handler :as service]
            [ring.server.standalone :refer [serve] ]
            )
  (:import [org.openrdf.rio RDFFormat]))

(defonce server (atom nil))

(defn get-handler []
  ;; #'app expands to (var app) so that when we reload our code,
  ;; the server is forced to re-resolve the symbol in the var
  ;; rather than having its own copy. When the root binding
  ;; changes, the server picks it up without having to restart.
  (-> #'service/app
      ; Makes static assets in $PROJECT_DIR/resources/public/ available.
      (wrap-file "resources")
      ; Content-Type, Content-Length, and Last Modified headers for files in body
      (wrap-file-info)))

(defn start-server
  "used for starting the server in development mode from REPL"
  [& [port]]
  (let [port (if port (Integer/parseInt port) 3000)]
    (reset! server
            (serve (get-handler)
                   {:port port
                    :init service/init
                    :auto-reload? true
                    :destroy service/destroy
                    :join? false}))
    (println (str "You can view the site at http://localhost:" port))))

(defn stop-server []
  (service/destroy)
  (.stop @server)
  (reset! server nil))

(comment

  (time (import-data-to-draft! repo "http://eldis.com/" (statements "eldis.nt")))
  ;; "Elapsed time: 425656.547 msecs"
  ;; => "http://publishmydata.com/graphs/drafter/draft/084e1be8-ab27-4f17-be72-f5e903e75b72"
  (time (migrate-live! repo "http://publishmydata.com/graphs/drafter/draft/084e1be8-ab27-4f17-be72-f5e903e75b72"))
  ;;"Elapsed time: 536353.654 msecs"

  (query repo (str "SELECT * WHERE { GRAPH <http://eldis.com/> { ?s ?p ?o }} LIMIT 10"))

)
