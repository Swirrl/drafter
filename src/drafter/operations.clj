(ns drafter.operations
  (:require [swirrl-server.middleware.log-request :refer [make-response-logger]]))

(defn get-query-timeout-seconds [{:keys [operation-timeout]}]
  ;;NOTE: operation-timeout is specified in milliseconds
  (max 1 (int (Math/ceil (/ operation-timeout 1000)))))

(defn create-timeouts
  "Specifies the timeouts for an operation."
  [operation-timeout]
  {:operation-timeout operation-timeout})

(def default-timeouts
  "default timeouts for SPARQL operations - 4
  minutes for the entire operation."
  (create-timeouts 240000))
