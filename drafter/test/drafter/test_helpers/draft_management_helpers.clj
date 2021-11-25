(ns drafter.test-helpers.draft-management-helpers
  (:require [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.rdf.sparql :as sparql]
            [drafter.backend.draftset.operations :as dsops]))

(defn draft-exists?
  "Checks state graph to see if a draft graph exists"
  [db graph-uri]
  (let [qry (str "ASK WHERE {"
                 "  SELECT ?s WHERE {"
                 (mgmt/with-state-graph
                   "      ?live a <" drafter:ManagedGraph "> ;"
                   "            <" drafter:hasDraft "> <" graph-uri "> ."
                   "      <" graph-uri "> a <" drafter:DraftGraph "> ."
                   "  }")
                 "  LIMIT 1"
                 "}")]
    (sparql/eager-query db qry)))

(defn draft-graph-exists-for?
  "Returns whether a draft contains a draft graph for a given live graph URI"
  [repo draftset-ref graph-uri]
  (let [draft-graph-uri (dsops/find-draftset-draft-graph repo draftset-ref graph-uri)]
    (some? draft-graph-uri)))

(defn draft-graphs
  "Get all the draft graph URIs"
  [db]
  (let [query-str (str "SELECT ?draft WHERE {"
                       (mgmt/with-state-graph
                         "?live <" drafter:hasDraft "> ?draft .")
                       "}")
        results (sparql/eager-query db query-str)]
    (into #{} (map :draft results))))
