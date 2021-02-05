(ns drafter.rdf.jena.spec
  (:require [clojure.spec.alpha :as s]
            [drafter.rdf.jena :as jena])
  (:import [org.apache.jena.update UpdateRequest]))

(s/def ::jena/UpdateRequest #(instance? UpdateRequest %))
(s/def ::jena/JenaUpdateOperation #(satisfies? jena/JenaUpdateOperation %))

(s/fdef jena/->update
  :args (s/cat :update-operations (s/coll-of ::jena/JenaUpdateOperation))
  :ret ::jena/UpdateRequest)

(s/fdef jena/->update-string
  :args (s/cat :update-operations (s/coll-of ::jena/JenaUpdateOperation))
  :ret string?)
