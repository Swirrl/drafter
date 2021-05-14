(ns drafter.endpoint
  (:require [grafter-2.rdf.protocols :refer [->Quad ->Triple context map->Triple]]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.vocabularies.dcterms :refer :all]
            [grafter.vocabularies.rdf :refer :all])
  (:import [java.time OffsetDateTime]))

(def includes #{:all :owned :claimable})

(defn- latest [^OffsetDateTime x ^OffsetDateTime y]
  (if (.isAfter x y) x y))

(defn merge-endpoints
  "Modifies the updated-at time of a draftset endpoint to the latest
   of its modified time and that of the public endpoint"
  [public-endpoint draftset-endpoint]
  (if public-endpoint
    (-> draftset-endpoint
        (update :updated-at
                (fn [draftset-modified]
                  (latest (:updated-at public-endpoint) draftset-modified)))
        (assoc :public-version (:version public-endpoint)))
    draftset-endpoint))
