(ns drafter-client.client.util
  (:require [clj-time.format :refer [formatters parse]]
            [clj-time.core :as t])
  (:import [java.util UUID]))

(defn date-time [s]
  (some->> s (parse (formatters :date-time))))

(defn now []
  (t/now))

(defn uuid [s]
  (some-> s UUID/fromString (try (catch Throwable _))))
