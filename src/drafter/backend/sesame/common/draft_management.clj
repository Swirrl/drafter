(ns drafter.backend.sesame.common.draft-management
  (:require [clojure.tools.logging :as log]
            [clojure.string :as string]
            [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add]]
            [drafter.util :as util]
            [drafter.rdf.draft-management :refer [drafter-state-graph update! xsd-datetime] :as mgmt]
            [drafter.backend.sesame.common.protocols :refer :all])
  (:import [java.util Date]))

(defn- sparql-uri-list [uris]
  (string/join " " (map #(str "<" % ">") uris)))

(defn ->sparql-values-binding [e]
  (if (coll? e)
    (str "(" (string/join " " e) ")")
    e))

(defn- meta-pair->values-binding [[uri value]]
  [(str "<" uri ">") (str \" value \")])

(defn meta-pairs->values-bindings [meta-pairs]
  (let [uri-pairs (map meta-pair->values-binding meta-pairs)]
    (string/join " " (map ->sparql-values-binding uri-pairs))))

(defn- append-metadata-to-graphs-query [graph-uris meta-pairs]
  (str "WITH <" drafter-state-graph ">
        DELETE { ?g ?p ?existing }
        INSERT { ?g ?p ?o }
        WHERE {
          VALUES ?g { " (sparql-uri-list graph-uris) " }
          VALUES (?p ?o) { " (meta-pairs->values-bindings meta-pairs)  " }
          OPTIONAL { ?g ?p ?existing }
        }"))

(defn append-metadata-to-graphs! [backend graph-uris meta-pairs]
  (let [update-query (append-metadata-to-graphs-query graph-uris meta-pairs)]
    (update! backend update-query)))
