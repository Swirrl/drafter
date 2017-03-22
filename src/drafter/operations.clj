(ns drafter.operations
  (:require [swirrl-server.middleware.log-request :refer [make-response-logger]]))

(defn get-query-timeout-seconds [operation-timeout]
  ;;NOTE: operation-timeout is specified in milliseconds
  (max 1 (int (Math/ceil (/ operation-timeout 1000)))))

(def default-timeouts
  "default timeouts for SPARQL operations - 4
  minutes for the entire operation."
  240000)
