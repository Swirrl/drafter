(ns drafter.test-helpers.draft-management-helpers
  (:require [drafter.rdf.draft-management :as mgmt :refer [query with-state-graph]]))

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
