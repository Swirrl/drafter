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
  (:import [org.openrdf.rio RDFFormat])
  (:gen-class))

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
  (let [port (if port (Integer/parseInt port) 3001)]
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


(defn -main [& args]
  (start-server))
