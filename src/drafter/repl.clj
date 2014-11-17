(ns drafter.repl
  (:use
   [ring.middleware file-info resource])
  (:require [grafter.rdf.sesame :refer [query prepare-query evaluate with-transaction]]
            [drafter.rdf.draft-management :refer :all]
            [drafter.handler :as service]
            [clojure.java.io :as io]
            [ring.server.standalone :refer [serve]]
            [environ.core :refer [env]])
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
      (wrap-resource "public")
      ; Content-Type, Content-Length, and Last Modified headers for files in body
      (wrap-file-info)))

(defn start-server
  "Used for starting the server in development mode from REPL"
  [port]
  (reset! server
          (serve (get-handler)
                 {:port port
                  :init service/init
                  :auto-reload? true
                  :open-browser? (:dev env)
                  :destroy service/destroy
                  :join? false}))

  (println (str "You can view the site at http://localhost:" port)))

(defn stop-server []
  (service/destroy)
  (.stop @server)
  (reset! server nil))

(defn ->int [i]
  (when i
    (if (integer? i)
      i
      (Integer/parseInt i))))

(defn -main []
  (start-server (or (->int (:drafter-http-port env))
                    3001)))
