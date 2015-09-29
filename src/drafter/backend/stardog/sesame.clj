(ns drafter.backend.stardog.sesame
  (:require [grafter.rdf.protocols :as proto]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.stardog.draft-api :as api]
            [drafter.backend.sesame.common.protocols :as sesproto]
            [drafter.backend.stardog.sesame.repository :refer [get-stardog-repo]]
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
  DraftManagement sparql-impl/draft-management-impl
  ApiOperations (assoc default-api-operations-impl
                       :delete-graph-job api/delete-graph-job)
  Stoppable default-stoppable-impl
  sesproto/ToRepository {:->sesame-repo :repo})

(defn get-backend [env-map]
  (let [repo (get-stardog-repo env-map)]
    (->StardogSesameBackend repo)))
