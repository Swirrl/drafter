(ns drafter.rdf.sparql
  (:require [clojure.tools.logging :as log]
            [drafter.rdf
             [drafter-ontology :refer :all]]
            [grafter.rdf
             [protocols :as pr]
             [repository :as repo]]))

(def prefixes (str "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX qb: <http://purl.org/linked-data/cube#> PREFIX skos: <http://www.w3.org/2004/02/skos/core#> PREFIX drafter: <" (drafter "") ">"))

(defn- mapply [f & args]
  (apply f (apply concat (butlast args) (last args))))

(defn query [repo query-str & {:as opts}]
  (log/info "Running query:"  query-str)
  (mapply repo/query repo (str prefixes query-str) opts))

(defn update! [repo update-string]
  (let [update-string (str prefixes update-string)]
    (log/info "Running update: " update-string)
    (pr/update! repo update-string)))
