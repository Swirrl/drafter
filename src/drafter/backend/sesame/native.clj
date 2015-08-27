(ns drafter.backend.sesame.native
  (:require [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame.common :refer :all]
            [drafter.backend.sesame.common.protocols :refer :all]
            [drafter.backend.sesame.native.repository :refer [get-repository]]
            [drafter.backend.sesame.native.draft-management :as mgmt]
            [grafter.rdf.protocols :as proto]))

(defrecord SesameNativeBackend [repo])

(extend SesameNativeBackend
  proto/ITripleReadable default-triple-readable-impl
  proto/ITripleWriteable default-triple-writeable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor default-sparql-update-impl
  ApiOperations default-api-operations-impl
  DraftManagement (assoc default-draft-management-impl :migrate-graphs-to-live! mgmt/migrate-graphs-to-live!)
  Stoppable default-stoppable-impl

  SesameBatchOperations default-sesame-batch-operations-impl)

(def get-backend-for-repo ->SesameNativeBackend)

(defn get-native-backend [env-map]
  (get-backend-for-repo (get-repository env-map)))
