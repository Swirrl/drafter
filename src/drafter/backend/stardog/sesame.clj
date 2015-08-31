(ns drafter.backend.stardog.sesame
  (:require [grafter.rdf.protocols :as proto]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame.common.protocols :refer :all]
            [drafter.backend.stardog.sesame.repository :refer [get-stardog-repo]]
            [drafter.backend.sesame.sparql.impl :as sparql-impl]
            [drafter.backend.sesame.common :refer :all]))

(defrecord StardogSesameBackend [repo])

(extend StardogSesameBackend
  proto/ITripleReadable default-triple-readable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor sparql-impl/sparql-update-executor-impl
  DraftManagement sparql-impl/draft-management-impl
  ApiOperations default-api-operations-impl
  Stoppable default-stoppable-impl
  ToRepository {:->sesame-repo :repo}

  ;TODO: remove? required by the default delete-graph-job implementation which deletes
  ;;in batches. This could be a simple DROP on Stardog
  SesameBatchOperations default-sesame-batch-operations-impl)

(defn get-stardog-backend [env-map]
  (let [repo (get-stardog-repo env-map)]
    (->StardogSesameBackend repo)))
