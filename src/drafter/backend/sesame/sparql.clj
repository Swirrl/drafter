(ns drafter.backend.sesame.sparql
  (:require [grafter.rdf.protocols :as proto]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame.common.protocols :refer :all]
            [drafter.backend.sesame.sparql.repository :refer [create-repository-for-environment]]
            [drafter.backend.sesame.sparql.impl :as sparql-impl]
            [drafter.backend.sesame.common :refer :all]))

(defrecord SesameSparqlBackend [repo])

(extend SesameSparqlBackend
  proto/ITripleReadable default-triple-readable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor sparql-impl/sparql-update-executor-impl
  DraftManagement sparql-impl/sparql-update-executor-impl
  ApiOperations default-api-operations-impl
  Stoppable default-stoppable-impl
  ToRepository {:->sesame-repo :repo}
  SesameBatchOperations default-sesame-batch-operations-impl)

(defn get-sesame-sparql-backend [env-map]
  (let [repo (create-repository-for-environment env-map)]
    (->SesameSparqlBackend repo)))
