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

(defn start-system!
  "Starts drafter with the supplied system.edn file (assumed to be in
  integrant & aero format).

  If no argument is given it will start drafter using the default
  system.edn resource."
  
  ([] (start-system! (io/resource "system.edn")))
  ([system-config]
   (println "Starting Drafter")
   (let [cfg (load-system system-config)]
     (alter-var-root #'system (constantly cfg)))))

(defn stop-system! []
  (log/info "Stopping Drafter")
  (when system
    (ig/halt! system)))

(defn add-shutdown-hook!
  "Register a shutdown hook with the JVM.  This is not guaranteed to
  be called in all circumstances, but should be called upon receipt of
  a SIGTERM (a normal Unix kill command).  It gives us an opportunity
  to try and shut things down gracefully."
  []
  (.addShutdownHook (Runtime/getRuntime) (Thread. stop-system!)))

(defn -main [& args]
  (add-shutdown-hook!)
  (if-let [config-file (first args)]
    (start-system! config-file)
    (start-system!)))
