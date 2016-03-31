(ns drafter.backend.sesame.remote
  (:require [grafter.rdf.protocols :as proto]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame.common.protocols :as sesproto]
            [drafter.backend.sesame.remote.repository :refer [create-repository-for-environment]]
            [drafter.backend.sesame.common :refer :all]))

(defrecord SesameRemoteSparqlBackend [repo])

(extend drafter.backend.sesame.remote.SesameRemoteSparqlBackend
  proto/ITripleReadable default-triple-readable-impl
  proto/ITripleWriteable default-triple-writeable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  SparqlUpdateExecutor default-sparql-update-impl
  ToRepository {:->sesame-repo :repo})

(defn get-backend [env-map]
  (let [repo (create-repository-for-environment env-map)]
    (->SesameRemoteSparqlBackend repo)))
