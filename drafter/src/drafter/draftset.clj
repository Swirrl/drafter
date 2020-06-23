(ns drafter.draftset
  "In memory clojure representations of Draftset objects and functions
  to operate on them."
  (:require [drafter.util :as util]
            [clojure.spec.alpha :as s]
            [drafter.rdf.drafter-ontology :as ont]
            [grafter.url :as url]
            [drafter.endpoint :as ep])
  (:import java.net.URI
           [java.util Date UUID]))

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

(s/def ::EmailAddress util/validate-email-address)
(s/def ::status #{:created :updated :deleted})
(s/def ::changes (s/map-of ::ep/URI (s/keys :req-un [::status])))
(s/def ::created-by ::EmailAddress)
(s/def ::display-name string?)
(s/def ::description string?)
(s/def ::submitted-by ::EmailAddress)
(s/def ::current-owner ::EmailAddress)
(s/def ::claim-user string?)
(s/def ::claim-role keyword?)

(s/def ::HasDescription (s/keys :req-un [::description]))
(s/def ::HasDisplayName (s/keys :req-un [::display-name]))
(s/def ::DraftsetCommon (s/merge ::ep/Endpoint
                                 (s/keys :req-un [::changes ::created-by]
                                         :opt-un [::display-name ::description ::submitted-by])))
(s/def ::OwnedDraftset (s/merge ::DraftsetCommon (s/keys :req-un [::current-owner])))
(s/def ::SubmittedToRole (s/keys :req-un [::claim-role]))
(s/def ::SubmittedToUser (s/keys :req-un [::claim-user]))
(s/def ::SubmittedDraftset (s/and ::DraftsetCommon (s/or :role ::SubmittedToRole :user ::SubmittedToUser)))
(s/def ::Draftset (s/or :owned ::OwnedDraftset :submitted ::SubmittedDraftset))

;; NOTE: this is currently only used only by tests
;;
;; TODO: Make the application use this function when loading a
;; draftset out of the database, and validate it conforms to our
;; in-memory/json schema.
(defn create-draftset
  ([creator]
   (let [created-at (Date.)]
     {:id (->DraftsetId (str (UUID/randomUUID)))
      :type "Draftset"
      :created-by creator
      :created-at created-at
      :modified-at created-at
      :current-owner creator}))
  ([creator display-name]
   (assoc (create-draftset creator) :display-name display-name))
  ([creator display-name description]
   (assoc (create-draftset creator display-name) :description description)))

(s/fdef create-draftset
        :args (s/cat :creator ::EmailAddress :display-name (s/? string?) :description (s/? string?))
  :ret ::OwnedDraftset)

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

;; NOTE: This var is currently unreferenced, we should probably try
;; and use it - tying it into either a SPEC/validation or something.
;;
;; It certainly seems useful to have an explicit list of expected
;; values.
;;
;; TODO consider making this a schema enum
(def operations #{:delete :edit :submit :publish :claim})
