(ns drafter-client.client.endpoint
  (:require [drafter-client.client.util :as util])
  (:import java.net.URI))

(defprotocol IEndpointRef
  (endpoint-id [this]))

(defrecord EndpointRef [id]
  IEndpointRef
  (endpoint-id [this] id))

(defprotocol IEndpoint
  (created-at [this])
  (updated-at [this])
  (version [this]))

(defrecord Endpoint [id created-at updated-at version]
  IEndpointRef
  (endpoint-id [this] id)
  IEndpoint
  (created-at [this] created-at)
  (updated-at [this] updated-at)
  (version [this] version))

(defn- ->ref [id] (->EndpointRef id))
(def public (->ref "public"))

(defn public-ref? [r]
  (= "public" (endpoint-id r)))

(defmulti from-json (fn [endpoint] (keyword (:type endpoint))))

(defmethod from-json :Endpoint [json]
  (-> json
      (update :created-at util/date-time)
      (update :updated-at util/date-time)
      (update :version #(URI. %))
      (map->Endpoint)))


