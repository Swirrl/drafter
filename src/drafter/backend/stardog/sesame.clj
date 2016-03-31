(ns drafter.backend.stardog.sesame
  (:require [grafter.rdf.protocols :as proto]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame.remote.repository :refer [create-repository-for-environment]]
            [drafter.backend.sesame.common :refer :all]))

(defrecord StardogSesameBackend [repo])

(extend drafter.backend.stardog.sesame.StardogSesameBackend
  proto/ITripleWriteable default-triple-writeable-impl
  proto/ITripleReadable default-triple-readable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  SparqlUpdateExecutor default-sparql-update-impl
  Stoppable default-stoppable-impl
  ToRepository {:->sesame-repo :repo})

(defn get-backend [env-map]
  (let [repo (create-repository-for-environment env-map)]
    (->StardogSesameBackend repo)))
