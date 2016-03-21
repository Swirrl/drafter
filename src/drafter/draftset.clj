(ns drafter.draftset
  (:require [schema.core :as s]
            [drafter.util :as util])
  (:import [java.net URI]
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
         {:claim-role s/Keyword}))

(def Draftset (s/either OwnedDraftset SubmittedDraftset))

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

(defn submit-to-role [draftset submitter role]
  (-> draftset
      (dissoc :current-owner)
      (assoc :claim-role role)
      (assoc :submitted-by submitter)))

(defn claim [draftset claimant]
  (-> draftset
      (dissoc :claim-role)
      (assoc :current-owner claimant)))

(def operations #{:delete :edit :submit :publish :claim})
