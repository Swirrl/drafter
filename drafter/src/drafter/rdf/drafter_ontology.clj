(ns drafter.rdf.drafter-ontology
  (:require
   [grafter.url :as url]
   [grafter.vocabularies.dcterms :refer :all])
  (:import [java.net URI]))

(def pmd-graphs (URI. "http://publishmydata.com/graphs/"))
(def modified-times-graph-uri (URI. "http://publishmydata.com/graphs/drafter/graph-modified-times"))

(def drafter (URI. "http://publishmydata.com/def/drafter/"))
(def draftset-uri (URI. "http://publishmydata.com/def/drafter/draftset/"))
(def drafter:endpoints (URI. "http://publishmydata.com/graphs/drafter/endpoints"))

(defn draftset-id->uri [id]
  (url/append-path-segments draftset-uri id))

(def submission-uri (url/->grafter-url "http://publishmydata.com/def/drafter/submission/"))

(defn submission-id->uri [id]
  (url/append-path-segments submission-uri id))

(def drafter:ManagedGraph (url/append-path-segments drafter "ManagedGraph"))

(def drafter:DraftGraph (url/append-path-segments drafter "DraftGraph"))

(def drafter:DraftSet (url/append-path-segments drafter "DraftSet"))

(def drafter:Endpoint (url/append-path-segments drafter "Endpoint"))

(def drafter:Submission (url/append-path-segments drafter "Submission"))

(def drafter:inDraftSet (url/append-path-segments drafter "inDraftSet"))

(def drafter:isPublic (url/append-path-segments drafter "isPublic"))

(def drafter:hasDraft (url/append-path-segments drafter "hasDraft"))

(def drafter:hasOwner (url/append-path-segments drafter "hasOwner"))

(def drafter:hasSubmission (url/append-path-segments drafter "hasSubmission"))

(def drafter:createdAt dcterms:created)

(def drafter:createdBy dcterms:creator)

(def drafter:modifiedAt dcterms:modified)

(def drafter:claimRole (url/append-path-segments drafter "claimRole"))

(def drafter:claimUser (url/append-path-segments drafter "claimUser"))

(def drafter:submittedBy (url/append-path-segments drafter "submittedBy"))

(def drafter:draft (url/append-path-segments drafter))

(def drafter:public (url/append-path-segments drafter "public"))

(def drafter:version (url/append-path-segments drafter "version"))

(def drafter:drafts (url/append-path-segments pmd-graphs "drafter" "drafts"))

(defn draft:graph [graph-id]
  (url/append-path-segments pmd-graphs "drafter" "draft" graph-id))

(def drafter:graph-modified (url/append-path-segments drafter "graph-modified"))
