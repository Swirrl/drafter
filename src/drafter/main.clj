(ns drafter.main
  (:require [integrant.core :as ig]
            [aero.core :as aero]
            [clojure.tools.logging :as log]
            [drafter.common.json-encoders :as enc]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.writers :as writers]
            [clojure.java.io :as io]
            [cognician.dogstatsd :as datadog]
            [clojure.edn :as edn])
  (:gen-class))

(require 'drafter.configuration)

(def system nil)

(defmethod aero/reader 'ig/ref [_ _ value]
  (ig/ref value))

(defmethod ig/init-key :drafter.main/datadog [k {:keys [statsd-address tags]}]
  (println "Initialising datadog")
  (datadog/configure! statsd-address {:tags tags}))

(def pre-initialised-keys #{:drafter/logging :drafter.main/datadog})

(defn initialisation-side-effects! [config]
  ;;(println "Initialisation Effects")
  (ig/load-namespaces config)
  (ig/init config pre-initialised-keys)

  ;; TODO consider moving these calls into more specific components
  (enc/register-custom-encoders!)
  (writers/register-custom-rdf-writers!)
  (jobs/init-job-settings! config))

(defn read-system [system-config]
  (if (map? system-config)
    system-config
    (-> system-config
        aero/read-config)))

(defn- load-system
  "Initialises the given system map."
  [system-config sys-keys]

  (initialisation-side-effects! system-config)
  
  (let [start-keys (->> (or sys-keys (keys system-config))
                        (remove pre-initialised-keys))]
    (ig/init system-config start-keys)))

(defn start-system!
  "Starts drafter with the supplied system.edn file (assumed to be in
  integrant & aero format).

  If no argument is given it will start drafter using the default
  system.edn resource.

  If a single arg is given it will load the given resource, file or if
  it's a map use its value as the system and start it.

  If two args are given the second argument is expected to be a set of
  keys to start from within the given system."
  
  ([] (start-system! (io/resource "system.edn")))
  ([sys] (start-system! sys nil))
  ([system-config start-keys]
   (println "Starting Drafter")
   (let [system (read-system system-config)
         initialised-sys (load-system system start-keys)]
     (alter-var-root #'system (constantly initialised-sys)))))

(defn stop-system! []
  (log/info "Stopping Drafter")
  (when system
    (ig/halt! system)))

(defn restart-system!
  "Note this doesn't do a refresh yet."
  []
  (stop-system!)
  (start-system!))

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
