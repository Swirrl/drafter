(ns drafter.backend.repository
  (:require [drafter.backend.protocols :refer :all]
            [drafter.rdf.sesame :as ses])
  (:import org.openrdf.repository.Repository))

(extend-type Repository
  SparqlExecutor
  (prepare-query [this sparql-string]
    (ses/prepare-query this sparql-string))

  SparqlUpdateExecutor
  (prepare-update [r update-query]
    (ses/prepare-update r update-query))

  ToRepository
  (->sesame-repo [r] r))
