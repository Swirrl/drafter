(ns drafter.feature.draftset-data.test-helper
  (:require [clojure.test :as t]
            [drafter.rdf.drafter-ontology
             :as do
             :refer
             [drafter:DraftGraph
              drafter:hasDraft
              drafter:inDraftSet
              drafter:modifiedAt]]
            [drafter.rdf.sparql :as sparql]))

(defn apply-job!
  "Execute the job in this thread"
  [{fun :function :as job}]
  (let [ret (fun job)]
    (t/is (= true ret)
          "Successful job (returns true doesn't return an exception/error)")))

(defn ensure-draftgraph-and-draftset-modified
  "Test that the draftgraph and draftset modified times match.  NOTE
  that because a draftset contains multiple draft graphs these and we
  expect the cardinality of modifiedAt to be 1 per resource this
  should only be applied after modifying a specific graph."
  [backend draftset live-graph]
  (let [ds-uri (do/draftset-id->uri (:id draftset))
        modified-query (str "SELECT ?modified {"
                            "   <" live-graph "> <" drafter:hasDraft "> ?draftgraph ."
                            "   ?draftgraph a <" drafter:DraftGraph "> ;"
                            "                 <" drafter:inDraftSet "> <" ds-uri "> ;"
                            "                 <" drafter:modifiedAt "> ?modified ."
                            "<" ds-uri ">" "<" drafter:modifiedAt "> ?modified ."
                            "} LIMIT 2")]

    (let [res (-> backend
                  (sparql/eager-query modified-query))]

      (assert (= 1 (count res)) "There were multiple modifiedAt timestamps, we expect just one.")

      (-> res first :modified))))




