(ns drafter.draftset
  (:require [schema.core :as s])
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
      (->DraftsetId (.toString relative)))))

(def ^:private SchemaCommon
  {:id (s/protocol DraftsetRef)
   :created-by s/Str
   :created-date Date
   (s/optional-key :display-name) s/Str
   (s/optional-key :description) s/Str})

(def OwnedDraftset
  (merge SchemaCommon
         {:current-owner s/Str}))

(def SubmittedDraftset
  (merge SchemaCommon
         {:claim-role s/Keyword}))

(def Draftset (s/either OwnedDraftset SubmittedDraftset))

(s/defn create-draftset :- Draftset
  ([creator :- s/Str]
   {:id (->DraftsetId (str (UUID/randomUUID)))
    :created-by creator
    :created-date (Date.)
    :current-owner creator})
  ([creator :- s/Str
    display-name :- s/Str]
   (assoc (create-draftset creator) :display-name display-name))
  ([creator :- s/Str
    display-name :- s/Str
    description :- s/Str]
   (assoc (create-draftset creator display-name) :description description)))

(defn submit-to [draftset role]
  (-> draftset
      (dissoc :current-owner)
      (assoc :claim-role role)))

(defn claim [draftset claimant]
  (-> draftset
      (dissoc :claim-role)
      (assoc :current-owner claimant)))

(def operations #{:delete :edit :submit :publish :claim})
