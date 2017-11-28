(ns drafter.backend
  (:require [clojure.spec.alpha :as s]
            [integrant.core :as ig])
  (:import org.eclipse.rdf4j.repository.Repository))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Specs for backend config etc.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(s/def ::sparql-query-endpoint uri?)
(s/def ::sparql-update-endpoint uri?)

(s/def ::uncached-repo #(instance? Repository %))
(s/def ::stasher-repo #(instance? Repository %))

(defmulti endpoint-repo
  "Given a drafter backend and an endpoint id, return a repository on
  the endpoint."
  (fn [drafter endpoint-id]
    (if (keyword? endpoint-id)
      endpoint-id
      (type endpoint-id))))

(defmethod endpoint-repo ::live [drafter endpoint-id]
  ,,,)

(defmethod ig/init-key :drafter/backend [_ opts]
  opts)


