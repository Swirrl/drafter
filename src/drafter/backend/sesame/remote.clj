(ns drafter.backend.sesame.remote
  (:require [grafter.rdf.protocols :as proto]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame.remote.batching :as batching]
            [drafter.backend.sesame.common.protocols :as sesproto]
            [drafter.backend.sesame.remote.repository :refer [create-repository-for-environment]]
            [drafter.backend.sesame.remote.impl :as sparql-impl]
            [drafter.backend.sesame.common :refer :all]))

(defrecord SesameRemoteSparqlBackend [repo])

(extend drafter.backend.sesame.remote.SesameRemoteSparqlBackend
  proto/ITripleReadable default-triple-readable-impl
  proto/ITripleWriteable default-triple-writeable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor sparql-impl/sparql-update-executor-impl
  DraftManagement sparql-impl/draft-management-impl
  ApiOperations default-api-operations-impl
  Stoppable default-stoppable-impl
  sesproto/ToRepository {:->sesame-repo :repo}
  sesproto/SesameBatchOperations {:delete-graph-batch! batching/delete-graph-batch!})

(defn get-backend [env-map]
  (let [repo (create-repository-for-environment env-map)]
    (->SesameRemoteSparqlBackend repo)))
