(ns drafter.repl
  (:require [drafter.handler :as service]
            [environ.core :refer [env]]
            [clojure.string :as s]
            [ring.middleware.file-info :refer :all]
            [ring.middleware.resource :refer :all]
            [ring.server.standalone :refer [serve]]
            [drafter.configuration :refer [get-configuration]])
  (:gen-class)
  (:import (java.lang.management ManagementFactory)))

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
  [port-or-config]
  (let [{:keys [port is-dev]} (if (number? port-or-config)
                                {:port port-or-config :is-dev true}
                                port-or-config)]
    (reset! server
            (serve (get-handler)
                   {:port          port
                    :init          service/init
                    :auto-reload?  true
                    :stacktraces?  is-dev               ;; remove fancy error page in all
                    ;; but the dev env (Jetty will
                    ;; still display HTML though)
                    :open-browser? is-dev
                    :destroy       service/destroy
                    :join?         false}))
    (println (str "Started with PID: " (get-pid)))
    (println (str "You can view the site at http://localhost:" port))))


(defn stop-server []
  (service/destroy)
  (.stop @server)
  (reset! server nil))

(defn -main []
  (let [config (get-configuration)]
    (start-server config)))
