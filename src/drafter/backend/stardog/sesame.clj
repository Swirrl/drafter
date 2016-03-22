(ns drafter.backend.stardog.sesame
  (:require [grafter.rdf.protocols :as proto]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame.common.protocols :as sesproto]
            [drafter.backend.sesame.remote.repository :refer [create-repository-for-environment]]
            [drafter.backend.sesame.remote.impl :as sparql-impl]
            [drafter.backend.sesame.common :refer :all]))

(defrecord StardogSesameBackend [repo])

(extend drafter.backend.stardog.sesame.StardogSesameBackend
  proto/ITripleWriteable default-triple-writeable-impl
  proto/ITripleReadable default-triple-readable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor sparql-impl/sparql-update-executor-impl
  DraftManagement default-draft-management-impl
  ApiOperations default-api-operations-impl
  Stoppable default-stoppable-impl
  sesproto/ToRepository {:->sesame-repo :repo})

(defn get-backend [env-map]
  (let [repo (create-repository-for-environment env-map)]
    (->StardogSesameBackend repo)))
