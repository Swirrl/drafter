(ns drafter-client.client.draftset
  (:refer-clojure :exclude [name])
  (:import (java.util UUID)))

(def live ::live)

(defprotocol IDraftsetId
  (id [this]))

(defprotocol IDraftsetName
  (name [this]))

(defprotocol IDraftsetDescription
  (description [this]))

(defrecord Draftset [id name description]
  IDraftsetId
  (id [this] id)
  IDraftsetName
  (name [this] name)
  IDraftsetDescription
  (description [this] description))

(defn live? [context]
  (= ::live context))

(defn draftset? [context]
  (satisfies? IDraftsetId context))

(defn drafter-endpoint?
  "Predicate for testing if the given value is a value representing a
  drafter endpoint.  Currently returns true either draftset? or
  live?."
  [context]
  (or (live? context)
      (draftset? context)))
