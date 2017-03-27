(ns drafter.test-helpers.draft-management-helpers
  (:require [clojure.tools.logging :as log]
            [drafter.util :as util]
            [grafter.rdf :refer [add s]]
            [drafter.util :refer [map-values]]
            [clojure.walk :refer [keywordize-keys]]
            [clojure.set :as set]
            [drafter.rdf.draft-management :refer [delete-draft-state-query drafter-state-graph with-state-graph xsd-datetime] :as mgmt]
            [drafter.rdf.sparql :as sparql]
            [grafter.vocabularies.rdf :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf.protocols :as pr]
            [grafter.rdf.repository :as repo]
            [drafter.backend.protocols :refer [->repo-connection ->sesame-repo]]
            [grafter.rdf.templater :refer [add-properties graph]]
            [clojure.string :as string]
            [swirrl-server.errors :refer [ex-swirrl]]
            [schema.core :as s])
  (:import (java.util Date UUID)
           (java.net URI)
           (org.openrdf.model.impl URIImpl)))

(defn draft-exists?
  "Checks state graph to see if a draft graph exists"
  [db graph-uri]
  (let [qry (str "ASK WHERE {"
                 "  SELECT ?s WHERE {"
                 (with-state-graph
                 "      ?live a drafter:ManagedGraph ;"
                 "           drafter:hasDraft <" graph-uri "> ."
                 "      <" graph-uri "> a drafter:DraftGraph ."
                 "  }")
                 "  LIMIT 1"
                 "}")]
    (sparql/query db qry)))

(defn draft-graphs
  "Get all the draft graph URIs"
  [db]
  (let [query-str (str "SELECT ?draft WHERE {"
                       (with-state-graph
                         "?live drafter:hasDraft ?draft .")
                       "}")
        res (->> (sparql/query db
                        query-str)
                 (map #(str (get % "draft")))
                 (into #{}))]
    res))
