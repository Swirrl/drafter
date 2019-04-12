(ns drafter.main
  (:gen-class)
  (:require [aero.core :as aero]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [cognician.dogstatsd :as datadog]
            [drafter.common.json-encoders :as enc]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.writers :as writers]
            [integrant.core :as ig]
            [meta-merge.core :as mm]))

(require 'drafter.configuration)

(def default-production-config [(io/resource "drafter-base-config.edn") (io/resource "drafter-prod-config.edn")])

(def system nil)

(defmethod aero/reader 'ig/ref [_ _ value]
  (ig/ref value))

(defmethod aero/reader 'resource [_ _ value]
  (io/resource value))

(defmethod ig/init-key :drafter.main/datadog [k {:keys [statsd-address tags]}]
  (log/info "Initialising datadog")
  (datadog/configure! statsd-address {:tags tags}))

(defn initialisation-side-effects! [config]
  (ig/load-namespaces config)
  ;; TODO consider moving these calls into more specific components
  (enc/register-custom-encoders!)
  (writers/register-custom-rdf-writers!)
  (jobs/init-job-settings! config))

(defn read-system [system-config]
  (if (map? system-config)
    system-config
    (-> system-config
        aero/read-config)))

(defn- inject-logging [system-config]
  "Add logging to datadog, and add logging and datadog to everything else."
  (merge-with
   merge
   (zipmap (keys (dissoc system-config :drafter/logging :drafter.main/datadog))
           (repeat (merge (when (contains? system-config :drafter/logging)
                            {:logging (ig/->Ref :drafter/logging)})
                          (when (contains? system-config :drafter.main/datadog)
                            {:datadog (ig/->Ref :drafter.main/datadog)}))))
   (when (and (contains? system-config :drafter.main/datadog)
              (contains? system-config :drafter/logging))
     {:drafter.main/datadog {:logging (ig/->Ref :drafter/logging)}})
   system-config))

(defn load-system
  "Initialises the given system map."
  [system-config sys-keys]
  (initialisation-side-effects! system-config)
  (let [start-keys (or sys-keys (keys system-config))
        system-config (inject-logging system-config)]
    (ig/init system-config start-keys)))


(defn start-system!
  "Starts drafter with the supplied edn config (assumed to be in
  integrant & aero format).

  If a single arg is given it will load the given resource, file or if
  it's a map use its value as the system and start it.

  If two args are given the second argument is expected to be a set of
  keys to start from within the given system."

  ([sys] (start-system! sys nil))
  ([system start-keys]
   (let [initialised-sys (load-system system start-keys)]
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

(defn resolve-configs
  "Merges the base config with any supplied config files.  Config files
  are assumed to be strings with relative file paths to edn/aero files
  we can read."
  [cfg-args]
  (let [profiles (concat [(io/resource "drafter-base-config.edn")]
                         (map io/file cfg-args))]
    (println "Using supplied configs " profiles)
    (map read-system profiles)))

(defn -main [& args]
  (println "Starting Drafter")
  (add-shutdown-hook!)
  (if (seq args)
    (start-system! (apply mm/meta-merge (resolve-configs args)))
    (start-system!
     ;; default production config
     (apply mm/meta-merge (->> default-production-config
                               (remove nil?)
                               (map read-system)))

     )))
