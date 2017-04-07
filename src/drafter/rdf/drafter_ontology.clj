(ns drafter.rdf.drafter-ontology
  (:require [grafter.url :as url]
            [grafter.vocabularies.dcterms :refer :all])
  (:import [java.net URI]))

(def drafter (URI. "http://publishmydata.com/def/drafter/"))
;(def drafter (prefixer "http://publishmydata.com/def/drafter/"))

;(def draftset-uri (prefixer "http://publishmydata.com/def/drafter/draftset/"))
(def draftset-uri (URI. "http://publishmydata.com/def/drafter/draftset/"))
(defn draftset-id->uri [id]
  (url/append-path-segments draftset-uri id))

;(def submission-uri (prefixer "http://publishmydata.com/def/drafter/submission/"))
(def submission-uri (url/->grafter-url "http://publishmydata.com/def/drafter/submission/"))

(defn submission-id->uri [id]
  (url/append-path-segments submission-uri id))
;
;(def drafter:ManagedGraph (drafter "ManagedGraph"))
(def drafter:ManagedGraph (url/append-path-segments drafter "ManagedGraph"))
;
;(def drafter:DraftGraph (drafter "DraftGraph"))
(def drafter:DraftGraph (url/append-path-segments drafter "DraftGraph"))
;
;(def drafter:DraftSet (drafter "DraftSet"))
(def drafter:DraftSet (url/append-path-segments drafter "DraftSet"))
;(def drafter:Submission (drafter "Submission"))
(def drafter:Submission (url/append-path-segments drafter "Submission"))
;
;(def drafter:inDraftSet (drafter "inDraftSet"))
(def drafter:inDraftSet (url/append-path-segments drafter "inDraftSet"))
;
;(def drafter:isPublic (drafter "isPublic"))
(def drafter:isPublic (url/append-path-segments drafter "isPublic"))
;
;(def drafter:hasDraft (drafter "hasDraft"))
(def drafter:hasDraft (url/append-path-segments drafter "hasDraft"))
;
;(def drafter:hasOwner (drafter "hasOwner"))
(def drafter:hasOwner (url/append-path-segments drafter "hasOwner"))
;(def drafter:hasSubmission (drafter "hasSubmission"))
(def drafter:hasSubmission (url/append-path-segments drafter "hasSubmission"))
;
(def drafter:createdAt dcterms:created)
;
(def drafter:createdBy dcterms:creator)
;
(def drafter:modifiedAt dcterms:modified)
;
;(def drafter:claimRole (drafter "claimRole"))
(def drafter:claimRole (url/append-path-segments drafter "claimRole"))
;(def drafter:claimUser (drafter "claimUser"))
(def drafter:claimUser (url/append-path-segments drafter "claimUser"))
;
;(def drafter:submittedBy (drafter "submittedBy"))
(def drafter:submittedBy (url/append-path-segments drafter "submittedBy"))
