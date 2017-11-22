(ns drafter.backend.live
  (:require [drafter.backend.common :as bprot :refer [->sesame-repo]]
            [drafter.backend.draftset :as ds]
            [drafter.rdf.draft-management :as mgmt]
            [grafter.rdf.protocols :as proto]
            [grafter.rdf4j.repository :as repo]))

(defrecord RestrictedExecutor [inner restriction]
  bprot/SparqlExecutor

  (prepare-query [this query-string]
    (let [pquery (bprot/prep-and-validate-query inner query-string)]
      (bprot/apply-restriction pquery restriction)))

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
