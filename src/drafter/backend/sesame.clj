(ns drafter.backend.sesame
  (:require [clojure.tools.logging :as log]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame-common :refer [default-sparql-update-impl default-stoppable-impl default-sparql-query-impl
                                                   default-to-connection-impl default-sparqlable-impl default-triple-readable-impl
                                                   default-isparql-updatable-impl default-query-rewritable-impl]]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :as proto]))

(defrecord SesameSparqlExecutor [repo])

(extend SesameSparqlExecutor
  repo/ToConnection default-to-connection-impl
  proto/ITripleReadable default-triple-readable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor default-sparql-update-impl
  Stoppable default-stoppable-impl)
