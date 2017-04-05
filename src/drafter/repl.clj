(ns drafter.repl
  (:gen-class)
  (:require [clojure.string :as s]
            [drafter.handler :as service]
            [environ.core :refer [env]]
            [ring.middleware
             [file-info :refer :all]
             [resource :refer :all]]
            [ring.server.standalone :refer [serve]])
  (:import java.lang.management.ManagementFactory))

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

(defn- get-pid []
  (-> (ManagementFactory/getRuntimeMXBean)
      (.getName)
      (s/split #"@")
      (first)
      (read-string)))

(defn start-server
  "Used for starting the server in development mode from REPL"
  [port]
  (do
    (reset! server
            (serve (get-handler)
                   {:port          port
                    :init          service/init
                    :auto-reload?  true
                    :stacktraces?  (:dev env) ;; remove fancy error page in all
                                              ;; but the dev env (Jetty will
                                              ;; still display HTML though)
                    :open-browser? (:dev env)
                    :destroy       service/destroy
                    :join?         false}))
    (println (str "Started with PID: " (get-pid)))
    (println (str "You can view the site at http://localhost:" port))))


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
