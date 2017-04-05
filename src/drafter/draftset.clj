(ns drafter.draftset
  "In memory clojure representations of Draftset objects and functions
  to operate on them."
  (:require [drafter.util :as util]
            [schema.core :as s])
  (:import java.net.URI
           [java.util Date UUID]))

(defprotocol DraftsetRef
  (->draftset-uri [this])
  (->draftset-id [this]))

(defrecord DraftsetURI [uri]
  Object
  (toString [this] uri))

(defrecord DraftsetId [id]
  DraftsetRef
  (->draftset-uri [this] (->DraftsetURI (drafter.rdf.drafter-ontology/draftset-uri id)))
  (->draftset-id [this] this)

  Object
  (toString [this] id))

(extend-type DraftsetURI
  DraftsetRef
  (->draftset-uri [this] this)
  (->draftset-id [{:keys [uri]}]
    (let [base-uri (URI. (drafter.rdf.drafter-ontology/draftset-uri ""))
          relative (.relativize base-uri (URI. uri))]
      (->DraftsetId (str relative)))))

(def ^:private email-schema (s/pred util/validate-email-address))

(def ^:private SchemaCommon
  {:id (s/protocol DraftsetRef)
   :created-by email-schema
   :created-date Date
   (s/optional-key :display-name) s/Str
   (s/optional-key :description) s/Str
   (s/optional-key :submitted-by) email-schema})

(def OwnedDraftset
  (merge SchemaCommon
         {:current-owner email-schema}))

(def SubmittedDraftset
  (merge SchemaCommon
         {(s/optional-key :claim-user) s/Str
          (s/optional-key :claim-role) s/Keyword}))

(def Draftset (s/either OwnedDraftset SubmittedDraftset))

;; NOTE: this is currently only Used only by tests
;;
;; TODO: Make the application use this function when loading a
;; draftset out of the database, and validate it conforms to our
;; in-memory/json schema.
(s/defn create-draftset :- Draftset
  ([creator :- email-schema]
   {:id (->DraftsetId (str (UUID/randomUUID)))
    :created-by creator
    :created-date (Date.)
    :current-owner creator})
  ([creator :- email-schema
    display-name :- s/Str]
   (assoc (create-draftset creator) :display-name display-name))
  ([creator :- email-schema
    display-name :- s/Str
    description :- s/Str]
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


;; NOTE: This var is currently unreferenced, we should probably try
;; and use it - tying it into either a SPEC/validation or something.
;;
;; It certainly seems useful to have an explicit list of expected
;; values.
;;
;; TODO consider making this a schema enum
(def operations #{:delete :edit :submit :publish :claim})
