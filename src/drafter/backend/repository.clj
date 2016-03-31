(ns drafter.backend.repository
  (:require [drafter.backend.protocols :refer :all]
            [drafter.rdf.sesame :as ses]
            [drafter.backend.sesame.common.sparql-execution :as exec])
  (:import [org.openrdf.repository Repository]))

(extend-type Repository
  SparqlExecutor
  (prepare-query [this sparql-string]
    (ses/prepare-query this sparql-string))

  SparqlUpdateExecutor
  (execute-update [r update-query]
    (exec/execute-update r update-query))

  ToRepository
  (->sesame-repo [r] r))
