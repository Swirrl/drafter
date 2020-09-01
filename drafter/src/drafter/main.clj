(ns drafter.main
  (:gen-class)
  (:require [aero.core :as aero]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [drafter.timeouts :as timeouts]
            [clojure.tools.logging :as log]
            [cognician.dogstatsd :as datadog]
            [drafter.common.json-encoders :as enc]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.writers :as writers]
            [integrant.core :as ig]
            [meta-merge.core :as mm]))

(def base-config (io/resource "drafter-base-config.edn"))

(def system nil)

(defmethod aero/reader 'ig/ref [_ _ value]
  (ig/ref value))

(defmethod aero/reader 'resource [_ _ value]
  (io/resource value))

;;Reads a timeout setting from the configuration. Timeout configurations
;;are optional. Returns an exception if the timeout string is invalid.
(defmethod aero/reader 'timeout
  [opts tag value]
  (some-> value (timeouts/try-parse-timeout)))

;;Reads a configuration setting corresponding to a TCP port. If specified the
;;setting should be a string representation of a number in the valid range for
;;TCP ports i.e. (0 65536]. Returns an exception if the input is invalid.
(defmethod aero/reader 'port
  [opts tag value]
  (when (some? value)
    (try
      (let [port (Long/parseLong value)]
        (if (and (pos? port) (< port 65536))
          port
          (IllegalArgumentException. "Port must be in range (0 65536]")))
      (catch Exception ex
        ex))))


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
    system-config ;; support supplying systems as a literal value for dev/testing
    (-> system-config
        aero/read-config)))



(defn- inject-logging [system-config]
  "Add logging to datadog, and add logging and datadog to everything else."
  (merge-with
   mm/meta-merge
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
  (let [profiles (concat [base-config]
                         (map io/file cfg-args))]
    (println "Using supplied configs " profiles)
    (map read-system profiles)))

(defn build-config [configs]
  (apply mm/meta-merge (resolve-configs configs)))

(defn -main [& args]
  (println "Starting Drafter")
  (add-shutdown-hook!)
  (if (seq args)
    (start-system! (build-config args))
    (binding [*out* *err*]
      (println "You must provide an integrant file of additional config to start drafter.")
      (println)
      (println "e.g. drafter-dev-auth0.edn, drafter-prod-auth0.edn, drafter-dev-basic-auth-memory.edn etc...")
      (System/exit 1))))
