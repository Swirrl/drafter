(ns drafter-client.client.draftset
  (:refer-clojure :exclude [name])
  (:require [drafter-client.client.endpoint :as endpoint]
            [drafter-client.client.util :refer [uuid date-time] :as util])
  (:import [java.util UUID]))

(def live ::live)

(defprotocol IDraftset
  (id [this])
  (name [this])
  (description [this])
  (public-version [this]))

(defrecord Draftset [id name description created-at updated-at version public-version]
  IDraftset
  (id [this] id)
  (name [this] name)
  (description [this] description)
  (public-version [this] public-version)

  endpoint/IEndpointRef
  (endpoint-id [this] id)

  endpoint/IEndpoint
  (created-at [this] created-at)
  (updated-at [this] updated-at)
  (version [this] version))

(defn ->draftset
  ([] (->draftset (UUID/randomUUID) nil nil))
  ([name] (->draftset (UUID/randomUUID) name nil))
  ([name description] (->draftset (UUID/randomUUID) name description))
  ([id name description] (->draftset id name description (util/now) (util/now)))
  ([id name description created-at] (->draftset id name description created-at created-at))
  ([id name description created-at updated-at]
   (let [version (str "urn:uuid:" (UUID/randomUUID))]
     (->draftset id name description created-at updated-at version)))
  ([id name description created-at updated-at version]
   (->draftset id name description created-at updated-at version nil))
  ([id name description created-at updated-at version public-version]
   (->Draftset id name description created-at updated-at version public-version)))

(defn from-json [ds]
  (-> ds
      (update :id uuid)
      (update :created-at date-time)
      (update :updated-at date-time)
      (assoc :name (:display-name ds))
      (dissoc :display-name)
      (map->Draftset)))

(defmethod endpoint/from-json :Draftset [json]
  (from-json json))

(defn live? [context]
  (= ::live context))

(defn draft? [context]
  (instance? Draftset context))
