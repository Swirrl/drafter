(ns drafter.configuration
  "Namespace for loading and validating the configuration."
  (:require [clojure.string :as string]
            [drafter.timeouts :as timeouts]
            [aero.core :as aero]
            [drafter.util :as util]
            [clojure.java.io :as io]))

(defmethod aero/reader 'timeout
  [opts tag value]
  (when (some? value)
    (let [timeout-or-ex (timeouts/try-parse-timeout value)]
      (if (util/throwable? timeout-or-ex)
        timeout-or-ex
        timeout-or-ex))))

(defmethod aero/reader 'port
  [opts tag value]
  (when (some? value)
    (try
      (let [port (Long/parseLong value)]
        (if (pos? port)
          port
          (IllegalArgumentException. "Port must be in range (0 65536]")))
      (catch Exception ex
        ex))))

(defmethod aero/reader 'opt-nat
  [opts tag value]
  (cond (number? value)
        (if (pos? value)
          value
          (IllegalArgumentException. "Value must be > 0"))

        (nil? value) nil

        (string? value)
        (try
          (aero/reader {} 'opt-nat (Long/parseLong value))
          (catch Exception ex
            ex))

        :else
        (IllegalArgumentException. "Expected numeric or string value")))

(defn validate-configuration! [config]
  (let [invalid (filter (fn [[k v]] (util/throwable? v)) config)]
    (when-not (empty? invalid)
      (doseq [[key value] invalid]
        (.println *err* (format "Invalid value for configuration setting '%s': %s" (name key) (.getMessage value))))
      (let [msg (string/join (map #(.getMessage (second %)) invalid))]
        (throw (RuntimeException. (str "Configuration invalid: " msg)))))))

(defn get-configuration []
  (let [config (aero/read-config (io/resource "drafter-configuration-settings.edn"))]
    (validate-configuration! config)
    config))