(ns drafter.common.api-routes
  (:require [drafter.rdf.drafter-ontology :refer [meta-uri]]))

(defn meta-params
  "Given a hashmap of query parameters grab the ones prefixed meta-, strip that
  off, and turn into a URI"
  [query-params]

  (reduce (fn [acc [k v]]
            (let [k (name k)
                  param-name (subs k (inc (.indexOf k "-")) (.length k))
                  new-key (meta-uri param-name)]
              (assoc acc new-key v)))
          {}
          (select-keys query-params
                       (filter (fn [p]
                                 (.startsWith (name p) "meta-"))
                               (keys query-params)))))
