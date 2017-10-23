(ns drafter.main
  (:require [integrant.core :as ig]
            [aero.core :as aero]
            [clojure.tools.logging :as log]
            [drafter.common.json-encoders :as enc]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.writers :as writers]
            [clojure.java.io :as io]
            [cognician.dogstatsd :as datadog])
  (:gen-class))

(require 'drafter.configuration)

(def system nil)

(defmethod aero/reader 'ig/ref [_ _ value]
  (ig/ref value))

(defmethod ig/init-key :drafter.main/datadog [k {:keys [statsd-address tags]}]
  (println "Initialising datadog")
  (datadog/configure! statsd-address {:tags tags}))

(def pre-init-keys [:drafter/logging :drafter.main/datadog])

(defn initialisation-side-effects! [config]
  ;;(println "Initialisation Effects")
  (ig/load-namespaces config)
  (ig/init config pre-init-keys)

  ;; TODO consider moving these calls into more specific components
  (enc/register-custom-encoders!)
  (writers/register-custom-rdf-writers!)
  (jobs/init-job-settings! config))

(defn- load-system
  [system-config-path]
  (let [config (-> system-config-path
                   io/file
                   aero/read-config)]

    (initialisation-side-effects! config)
    (ig/init config (keys (apply dissoc config pre-init-keys)))))

(defn start-system! [system-config]
  (println "Starting Drafter")
  (let [cfg (load-system system-config)]
    (alter-var-root #'system (constantly cfg))))

(defn stop-system! []
  (log/info "Stopping Drafter")
  (when system
    (ig/halt! system)))

(defn add-shutdown-hook! []
  (.addShutdownHook (Runtime/getRuntime) (Thread. stop-system!)))

;; TODO
(defn -main [args]
  (add-shutdown-hook!)
  (start-system! "system.edn"))
