(ns drafter.test-helpers.draft-management-helpers
  (:require [drafter.backend.draftset.draft-management
             :as
             mgmt
             :refer
             [with-state-graph]]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.sparql :as sparql]
            [grafter-2.rdf4j.repository :as repo]))

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
    (sparql/eager-query db qry)))

(defn draft-graphs
  "Get all the draft graph URIs"
  [db]
  (let [query-str (str "SELECT ?draft WHERE {"
                       (with-state-graph
                         "?live <" drafter:hasDraft "> ?draft .")
                       "}")
        results (sparql/eager-query db query-str)]
    (into #{} (map :draft results))))
