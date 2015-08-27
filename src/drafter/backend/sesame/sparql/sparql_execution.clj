(ns drafter.backend.sesame.sparql.sparql-execution
  (:require [grafter.rdf.repository :as repo]
            [drafter.backend.sesame.common.protocols :refer [->sesame-repo]]
            [drafter.backend.sesame.common.sparql-execution :refer [create-execute-update-fn]]))

;;default sesame implementation execute UPDATE queries in a transaction which the remote SPARQL
;;client does not like
(def execute-update-fn
  (create-execute-update-fn #(->sesame-repo %) (fn [conn pquery] (repo/evaluate pquery))))
