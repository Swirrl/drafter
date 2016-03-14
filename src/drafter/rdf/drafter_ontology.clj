(ns drafter.rdf.drafter-ontology
  (:require [grafter.rdf :refer [prefixer]])
  (:require [grafter.vocabularies.dcterms :refer :all]))

(def drafter (prefixer "http://publishmydata.com/def/drafter/"))

(def draft-uri (prefixer "http://publishmydata.com/graphs/drafter/draft/"))

(def meta-uri (prefixer "http://publishmydata.com/def/drafter/meta/"))

(def draftset-uri (prefixer "http://publishmydata.com/def/drafter/draftset/"))

(def drafter:ManagedGraph (drafter "ManagedGraph"))

(def drafter:DraftGraph (drafter "DraftGraph"))

(def drafter:DraftSet (drafter "DraftSet"))

(def drafter:inDraftSet (drafter "inDraftSet"))

(def drafter:isPublic (drafter "isPublic"))

(def drafter:hasDraft (drafter "hasDraft"))

(def drafter:hasOwner (drafter "hasOwner"))

(def drafter:createdAt dcterms:created)

(def drafter:createdBy dcterms:creator)

(def drafter:modifiedAt dcterms:modified)

(def drafter:claimableBy (drafter "claimableBy"))

(def drafter:submittedBy (drafter "submittedBy"))
