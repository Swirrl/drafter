(ns drafter.main
  (:require [integrant.core :as ig]
            [aero.core :as aero]
            [clojure.java.io :as io]
            [cognician.dogstatsd :as datadog]))

(require 'drafter.configuration)

(def system)

(defmethod aero/reader 'ig/ref [_ _ value]
  (ig/ref value))

(defmethod ig/init-key :drafter.main/datadog [k {:keys [statsd-address tags]}]
  (datadog/configure! statsd-address {:tags tags})
  :side-effecting!)

(defn- load-system
  [system-config-path]
  (let [config (-> system-config-path
                   io/file
                   aero/read-config)]
    (ig/load-namespaces config)
    (ig/init config)))

(defn start-system! [system-config]
  (println "Starting Drafter")
  (let [cfg (load-system system-config)]
    (alter-var-root #'system (constantly cfg))))


(defn stop-system! []
  (println "Stopping Drafter")
  (ig/halt! system))

(defn add-shutdown-hook! []
  (.addShutdownHook (Runtime/getRuntime) (Thread. stop-system!)))

;; TODO
(defn -main [args]
  (add-shutdown-hook!)
  (start-system! "system.edn"))
