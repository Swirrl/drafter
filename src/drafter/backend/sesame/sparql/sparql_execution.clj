(ns drafter.backend.sesame.sparql.sparql-execution
  (:require [grafter.rdf.repository :as repo]
            [drafter.backend.sesame.common.protocols :refer [->sesame-repo]]
            [drafter.backend.sesame.common.sparql-execution :refer [execute-update-with]]))

;;default sesame implementation executes UPDATE queries in a transaction which the remote SPARQL
;;client does not like
(defn execute-update [backend update-query restrictions]
  (execute-update-with (fn [conn pquery] (repo/evaluate pquery)) backend update-query restrictions))
