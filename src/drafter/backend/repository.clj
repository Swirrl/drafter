(ns drafter.backend.repository
  "Extend drafter query protocols to our DrafterSPARQLRepository
  type."
  (:require [drafter.backend.protocols :refer :all]
            [drafter.rdf.sesame :as ses])
  (:import org.openrdf.repository.Repository))

(extend-type Repository
  SparqlExecutor
  (prepare-query [this sparql-string]
    (ses/prepare-query this sparql-string))

  ToRepository
  (->sesame-repo [r] r))
