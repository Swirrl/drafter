(ns drafter-client.client.draftset
  (:refer-clojure :exclude [name])
  (:import (java.util UUID)))

(def live ::live)

(defprotocol IDraftset
  (id [this])
  (name [this])
  (description [this]))

(defrecord Draftset [id name description]
  IDraftset
  (id [this] id)
  (name [this] name)
  (description [this] description))

(defn ->draftset
  ([] (->draftset (UUID/randomUUID) nil nil))
  ([name] (->draftset (UUID/randomUUID) name nil))
  ([name description] (->draftset (UUID/randomUUID) name description))
  ([id name description] (->Draftset id name description)))

(defn live? [context]
  (= ::live context))

(defn draft? [context]
  (instance? Draftset context))
(defn drafter-endpoint?
  "Predicate for testing if the given value is a value representing a
  drafter endpoint.  Currently returns true either draftset? or
  live?."
  [context]
  (or (live? context)
      (draftset? context)))
