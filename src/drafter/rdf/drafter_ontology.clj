(ns drafter.rdf.drafter-ontology
  (:require [grafter.rdf :refer [prefixer]])
  (:require [grafter.vocabularies.dcterms :refer [dcterms:issued dcterms:modified]]))

(def drafter (prefixer "http://publishmydata.com/def/drafter/"))

(def draft-uri (prefixer "http://publishmydata.com/graphs/drafter/draft/"))

(def meta-uri (prefixer "http://publishmydata.com/def/drafter/meta/"))

(def drafter:ManagedGraph (drafter "ManagedGraph"))

(def drafter:DraftGraph (drafter "DraftGraph"))

(def drafter:isPublic (drafter "isPublic"))

(def drafter:hasDraft (drafter "hasDraft"))

(def drafter:hasOwner (drafter "hasOwner"))

(def drafter:createdAt dcterms:issued)

(def drafter:modifiedAt dcterms:issued)
