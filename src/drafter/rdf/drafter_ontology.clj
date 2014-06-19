(ns drafter.rdf.drafter-ontology
  (:require [grafter.rdf :refer [prefixer]])
  (:require [grafter.rdf.ontologies.dcterms :refer [dcterms:issued dcterms:modified]]))

(def drafter (prefixer "http://publishmydata.com/def/drafter/"))

(def drafter:ManagedGraph (drafter "ManagedGraph"))

(def drafter:DraftGraph (drafter "DraftGraph"))

(def drafter:isPublic (drafter "isPublic"))

(def drafter:hasDraft (drafter "hasDraft"))

(def drafter:hasOwner (drafter "hasOwner"))

(def drafter:createdAt dcterms:issued)

(def drafter:modifiedAt dcterms:issued)
