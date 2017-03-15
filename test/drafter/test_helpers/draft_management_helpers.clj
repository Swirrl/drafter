(ns drafter.test-helpers.draft-management-helpers
  (:require [grafter.rdf :refer [add s]]
            [drafter.util :refer [map-values]]
            [clojure.walk :refer [keywordize-keys]]
            [drafter.rdf.draft-management :refer [update! delete-draft-state-query drafter-state-graph with-state-graph query xsd-datetime] :as mgmt]
            [grafter.vocabularies.rdf :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.backend.protocols :refer [->repo-connection ->sesame-repo]]
            [grafter.rdf.templater :refer [add-properties graph]]
            [swirrl-server.errors :refer [ex-swirrl]]))

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
    (query db qry)))

(defn draft-graphs
  "Get all the draft graph URIs"
  [db]
  (let [query-str (str "SELECT ?draft WHERE {"
                       (with-state-graph
                         "?live drafter:hasDraft ?draft .")
                       "}")
        res (->> (query db
                        query-str)
                 (map #(str (get % "draft")))
                 (into #{}))]
    res))
