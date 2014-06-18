(ns drafter.rdf.drafter-ontology
  (:require [grafter.rdf :refer [prefixer]]))

(def drafter (prefixer "http://publishmydata.com/def/drafter/"))

(def drafter:ManagedGraph (drafter "ManagedGraph"))

(def drafter:DraftGraph (drafter "DraftGraph"))

(def drafter:isPublic (drafter "isPublic"))

(def drafter:hasOwner (drafter "hasOwner"))
