(ns drafter.backend
  (:require [clojure.spec.alpha :as s])
  (:import org.eclipse.rdf4j.repository.Repository))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Specs for backend config etc.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::sparql-query-endpoint uri?)
(s/def ::sparql-update-endpoint uri?)

(s/def ::uncached-repo #(instance? Repository %))
(s/def ::stasher-repo #(instance? Repository %))
