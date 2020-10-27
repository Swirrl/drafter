(ns drafter.draftset
  "In memory clojure representations of Draftset objects and functions
  to operate on them."
  (:require
   [drafter.rdf.drafter-ontology :as ont]
   [drafter.util :as util]
   [grafter.url :as url])
  (:import java.net.URI
           [java.util UUID]
           [java.time OffsetDateTime]))

(defprotocol DraftsetRef
  (->draftset-uri [this])
  (->draftset-id [this]))

(extend-type URI
  DraftsetRef
  (->draftset-uri [this] this)
  (->draftset-id [this]
    (last (url/path-segments this))))

;; TODO consider removing DraftsetURI/DraftsetRef records they could probably just just be Draftset
(defrecord DraftsetURI [uri]
  url/IURIable
  (->java-uri [this] uri)

  Object
  (toString [this] (str uri)))

(defrecord DraftsetId [id]
  DraftsetRef
  (->draftset-uri [this] (->DraftsetURI (ont/draftset-id->uri id)))
  (->draftset-id [this] this)

  url/IURIable
  (->java-uri [this] (ont/draftset-id->uri id))

  Object
  (toString [this] id))

(extend-type String
  DraftsetRef
  (->draftset-uri [this] (url/->java-uri (ont/draftset-id->uri this)))
  (->draftset-id [this] (->DraftsetId this)))

(extend-type DraftsetURI
  DraftsetRef
  (->draftset-uri [this] this)
  (->draftset-id [{:keys [uri]}]
    (let [relative (.relativize ont/draftset-uri uri)]
      (->DraftsetId (str relative)))))

(defn new-id
  "Creates a new draftset id"
  []
  (->DraftsetId (str (UUID/randomUUID))))

;; NOTE: this is currently only used only by tests
;;
;; TODO: Make the application use this function when loading a
;; draftset out of the database, and validate it conforms to our
;; in-memory/json schema.
(defn create-draftset
  ([creator]
   (let [created-at (OffsetDateTime/now)]
     {:id (new-id)
      :type "Draftset"
      :changes {}
      :created-by creator
      :created-at created-at
      :updated-at created-at
      :version (util/version)
      :current-owner creator}))
  ([creator display-name]
   (assoc (create-draftset creator) :display-name display-name))
  ([creator display-name description]
   (assoc (create-draftset creator display-name) :description description)))

(defn- submit [draftset submitter role-or-user-key role-or-user-value]
  (-> draftset
      (dissoc :current-owner :claim-role :claim-user)
      (assoc role-or-user-key role-or-user-value)
      (assoc :submitted-by submitter)))

;; Not used outside of tests
(defn submit-to-role [draftset submitter role]
  (submit draftset submitter :claim-role role))

;; Not used outside of tests
(defn submit-to-user [draftset submitter username]
  (submit draftset submitter :claim-user username))

(defn claim [draftset claimant]
  (-> draftset
      (dissoc :submission)
      (assoc :current-owner claimant)))
