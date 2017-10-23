(ns drafter.server
  (:require [clojure.string :as s]
            [ring.server.standalone :refer [serve]]
            [integrant.core :as ig])
  (:gen-class)
  (:import (java.lang.management ManagementFactory)))

(defn- get-pid []
  (-> (ManagementFactory/getRuntimeMXBean)
      (.getName)
      (s/split #"@")
      (first)
      (read-string)))

(defmethod ig/init-key :drafter.server/http [_ {:keys [port handler stacktraces?]}]
  (let [server (serve handler
                      {:port          port
                       :init #(do (println (str "Started with PID: " (get-pid)))
                                  (println (str "You can view the site at http://localhost:" port)))
                       :auto-reload?  false
                       :stacktraces?  stacktraces? ;; remove fancy error page in all
                       
                       ;; but the dev env (Jetty will
                       ;; still display HTML though)
                       ;;:destroy       service/destroy
                       :join?         false})]
    server))

(defmethod ig/halt-key! :drafter.server/http [k server]
  (.stop server))

