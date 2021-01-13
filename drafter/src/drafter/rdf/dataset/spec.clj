(ns drafter.rdf.dataset.spec
  (:require [clojure.spec.alpha :as s]
            [drafter.rdf.dataset :as ds]))

(s/def ::ds/default-graphs (s/coll-of uri? :kind set?))
(s/def ::ds/named-graphs (s/coll-of uri? :kind set?))
(s/def ::ds/Dataset (s/keys :req-un [::default-graphs ::named-graphs]))
(s/def :rdf4j/Dataset #(instance? org.eclipse.rdf4j.query.Dataset %))

(s/fdef ds/->restricted-dataset
  :args (s/cat :dataset ::ds/Dataset)
  :ret :rdf4j/Dataset)

(s/fdef ds/get-query-dataset
  :args (s/cat :query-dataset ::ds/Dataset
               :user-dataset ::ds/Dataset
               :visible-graphs (s/coll-of uri? :kind set?))
  :ret :rdf4j/Dataset)


