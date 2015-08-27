(ns drafter.backend.stardog.sesame
  (:require [grafter.rdf.protocols :as proto]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame.common.protocols :refer :all]
            [drafter.backend.stardog.sesame.repository :refer [get-stardog-repo]]
            [drafter.backend.sesame.sparql.draft-management :as sparqlmgmt]
            [drafter.backend.sesame.sparql.sparql-execution :refer [execute-update-fn]]
            [drafter.backend.sesame.common :refer :all]))

(defrecord StardogSesameBackend [repo])

(extend StardogSesameBackend
  proto/ITripleReadable default-triple-readable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor {:execute-update execute-update-fn}
  DraftManagement (assoc default-draft-management-impl
                    :append-data-batch! sparqlmgmt/append-data-batch
                    :migrate-graphs-to-live! sparqlmgmt/migrate-graphs-to-live!)
  ApiOperations default-api-operations-impl
  Stoppable default-stoppable-impl
  ToRepository {:->sesame-repo :repo}

  ;TODO: remove? required by the default delete-graph-job implementation which deletes
  ;;in batches. This could be a simple DROP on Stardog
  SesameBatchOperations default-sesame-batch-operations-impl)

(defn get-stardog-backend [env-map]
  (let [repo (get-stardog-repo env-map)]
    (->StardogSesameBackend repo)))
