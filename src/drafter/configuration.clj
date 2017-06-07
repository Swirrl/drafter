(ns drafter.configuration
  "Namespace for loading and validating the configuration."
  (:require [clojure.string :as string]
            [drafter.timeouts :as timeouts]
            [aero.core :as aero]
            [drafter.util :as util]
            [clojure.java.io :as io]))

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

;;Reads an optional configuration setting representing a natural number.
;;The value to read can be either a number or a string representation of a
;;number. Returns an exception if the input is invalid.
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
  (let [config (aero/read-config (io/resource "drafter-config.edn"))]
    (validate-configuration! config)
    config))