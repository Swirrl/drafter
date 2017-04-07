(ns drafter.test-helpers.draft-management-helpers
  (:require [drafter.rdf
             [draft-management :as mgmt :refer [with-state-graph]]
             [sparql :as sparql]]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf.repository :as repo]))

(defn draft-exists?
  "Checks state graph to see if a draft graph exists"
  [db graph-uri]
  (let [qry (str "ASK WHERE {"
                 "  SELECT ?s WHERE {"
                 (with-state-graph
                 "      ?live a <" drafter:ManagedGraph "> ;"
                 "            <" drafter:hasDraft "> <" graph-uri "> ."
                 "      <" graph-uri "> a <" drafter:DraftGraph "> ."
                 "  }")
                 "  LIMIT 1"
                 "}")]
    (repo/query db qry)))

(defn draft-graphs
  "Get all the draft graph URIs"
  [db]
  (let [query-str (str "SELECT ?draft WHERE {"
                       (with-state-graph
                         "?live <" drafter:hasDraft "> ?draft .")
                       "}")
        results (repo/query db query-str)]
    (into #{} (map :draft results))))
