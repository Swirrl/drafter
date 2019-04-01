(ns drafter.backend.spec
  (:require [clojure.spec.alpha :as s])
  (:import org.eclipse.rdf4j.repository.Repository))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Specs for backend config etc.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::sparql-query-endpoint uri?)
(s/def ::sparql-update-endpoint uri?)

(s/def ::repo #(instance? Repository %))

