(ns drafter.backend.sesame.native
  (:require [drafter.backend.protocols :as backproto]
            [drafter.backend.sesame.common :refer :all]
            [drafter.backend.sesame.common.protocols :as sesproto]
            [drafter.backend.sesame.native.repository :refer [get-repository]]
            [drafter.backend.sesame.native.draft-management :as mgmt]
            [grafter.rdf.protocols :as proto]))

(defrecord SesameNativeStoreBackend [repo])

(extend drafter.backend.sesame.native.SesameNativeStoreBackend
  proto/ITripleReadable default-triple-readable-impl
  proto/ITripleWriteable default-triple-writeable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  backproto/SparqlExecutor default-sparql-query-impl
  backproto/QueryRewritable default-query-rewritable-impl
  backproto/SparqlUpdateExecutor default-sparql-update-impl
  backproto/ApiOperations default-api-operations-impl
  backproto/DraftManagement (assoc default-draft-management-impl :migrate-graphs-to-live! mgmt/migrate-graphs-to-live!)
  sesproto/ToRepository {:->sesame-repo :repo}
  backproto/Stoppable default-stoppable-impl
  sesproto/SesameBatchOperations default-sesame-batch-operations-impl)

(defn get-backend [env-map]
  (->SesameNativeStoreBackend (get-repository env-map)))
