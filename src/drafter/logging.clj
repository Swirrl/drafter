(ns drafter.logging
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [clojure.java.io :as io]))


(defn- import-logging! []
  ;; Note that though the classes and requires below aren't used in this
  ;; namespace they are needed by the log-config file which is loaded
  ;; from here.
  (import '[org.apache.log4j ConsoleAppender DailyRollingFileAppender RollingFileAppender EnhancedPatternLayout PatternLayout SimpleLayout]
          '[org.apache.log4j.helpers DateLayout]
          '[java.util UUID])

  (require '[clj-logging-config.log4j :refer [set-loggers!]]
           'drafter.errors))

(defn- load-logging-configuration [config-file]
  (-> config-file slurp read-string))

(def default-config-resource (io/resource "log-config.edn"))

(defn configure-logging! [config-file]
  (import-logging!)
  (binding [*ns* (find-ns 'drafter.logging)]
    (let [default-config (load-logging-configuration default-config-resource)
          config-file (when config-file
                        (load-logging-configuration (io/file config-file)))]

      (let [chosen-config (or config-file default-config)]
        (eval chosen-config)
        (log/debug "Loaded logging config" chosen-config)))))


(defmethod ig/init-key :drafter/logging [_ {:keys [config]}]
  (configure-logging! config))
