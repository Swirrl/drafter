(ns drafter-client.client.util
  (:require [clj-time.format :refer [formatters parse]]
            [clj-time.core :as t])
  (:import [java.util UUID]
           java.net.URI))

(defn- parse-legacy-drafter-date-time
  "Try parsing iso date-time without ms. This format was returned by
  legacy drafters and appears to have changed between drafter 2.2.2
  and 2.2.3 with the change over to java.time."
  [s]
  (some->> s (parse (formatters :date-time-no-ms))))

(defn date-time
  "Try to parse the string as an ISO date time with or without ms, e.g.
  should accept date-times like: 2020-10-16T08:51:59.000Z and
  2020-10-16T08:51:59Z.
  "
  [s]
  (try
    (some->> s (parse (formatters :date-time)))
    (catch IllegalArgumentException ex
      ;; Raised if there's a parse error. We should be able to remove
      ;; this after we've upgraded all deployments of drafter 2.2.2
      (parse-legacy-drafter-date-time s))))

(defn now []
  (t/now))

(defn uuid [s]
  (some-> s UUID/fromString (try (catch Throwable _))))

(defn version
  ([]
   (version (UUID/randomUUID)))
  ([id]
   (URI. (str "http://publishmydata.com/def/drafter/version/" id))))
