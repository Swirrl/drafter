(ns drafter.backend.live
  (:require [drafter.backend.protocols :as bprot :refer [->sesame-repo]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.draftset-management.operations :as dsmgmt]
            [drafter.rdf.rewriting.query-rewriting :refer [rewrite-sparql-string]]
            [drafter.rdf.rewriting.result-rewriting :refer [rewrite-query-results]]
            [drafter.rdf.sesame :refer [apply-restriction prepare-query]]
            [grafter.rdf.protocols :as proto]
            [grafter.rdf4j.repository :as repo]
            [schema.core :as s])
  (:import org.eclipse.rdf4j.model.URI
           org.eclipse.rdf4j.repository.Repository))

(defrecord RestrictedExecutor [inner restriction]
  bprot/SparqlExecutor

  (prepare-query [this query-string]
    (let [pquery (prepare-query inner query-string)]
      (apply-restriction pquery restriction)))

  bprot/ToRepository
  (->sesame-repo [_] (->sesame-repo inner)))

(extend RestrictedExecutor
  proto/ITripleReadable bprot/itriple-readable-delegate
  proto/ITripleWriteable bprot/itriple-writeable-delegate
  proto/ISPARQLable bprot/isparqlable-delegate
  proto/ISPARQLUpdateable bprot/isparql-updateable-delegate
  repo/ToConnection bprot/to-connection-delegate)

(defn live-endpoint
  "Creates a backend restricted to the live graphs."
  [backend]
  (->RestrictedExecutor backend (partial mgmt/live-graphs backend)))
