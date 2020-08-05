(ns drafter.endpoint
  (:require [grafter-2.rdf.protocols :refer [->Quad ->Triple context map->Triple]]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.vocabularies.dcterms :refer :all]
            [grafter.vocabularies.rdf :refer :all])
  (:import [java.time OffsetDateTime]))

(def includes #{:all :owned :claimable})

(defn public
  "Creates a public endpoint map with the created and modified dates. Uses the current
   time for these values if not specified."
  ([] (public (OffsetDateTime/now)))
  ([created] (public created created))
  ([created modified]
   {:id "public" :type "Endpoint" :created-at created :updated-at modified}))

(defn- latest [^OffsetDateTime x ^OffsetDateTime y]
  (if (.isAfter x y) x y))

(defn merge-endpoints
  "Modifies the updated-at time of a draftset endpoint to the latest
   of its modified time and that of the public endpoint"
  [{public-modified :updated-at :as public-endpoint} draftset-endpoint]
  (if public-endpoint
    (update draftset-endpoint :updated-at (fn [draftset-modified]
                                            (latest public-modified draftset-modified)))
    draftset-endpoint))
