(ns drafter.backend.sesame
  (:require [clojure.tools.logging :as log]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame-common :refer [default-sparql-update-impl default-stoppable-impl default-sparql-query-impl
                                                   default-to-connection-impl default-sparqlable-impl default-triple-readable-impl
                                                   default-isparql-updatable-impl]]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :as proto]))

(defrecord SesameSparqlExecutor [repo])

(extend SesameSparqlExecutor
  repo/ToConnection default-to-connection-impl
  proto/ITripleReadable default-triple-readable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  SparqlUpdateExecutor default-sparql-update-impl
  Stoppable default-stoppable-impl)

(defrecord RewritingSesameSparqlExecutor [inner query-rewriter result-rewriter]
  SparqlExecutor
  (prepare-query [_ sparql-string restrictions]
    (prepare-query inner (query-rewriter sparql-string) restrictions))

  (get-query-type [_ pquery]
    (get-query-type inner pquery))

  (negotiate-result-writer [_ pquery media-type]
    (if-let [inner-writer (negotiate-result-writer inner pquery media-type)]
      #(result-rewriter pquery (inner-writer %))))

  (create-query-executor [_ writer-fn pquery]
    (create-query-executor inner writer-fn pquery)))
