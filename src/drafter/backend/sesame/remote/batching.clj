(ns drafter.backend.sesame.remote.batching
  (:require [grafter.rdf.repository :as repo]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.backend.sesame.common.protocols :as proto]))

(defn delete-graph-batch!
  "Implementation for deleting a batch of statements from a graph.  On the
  Sesame remote backend this operation shouldn't be in a transaction, because of
  the difference in transaction behaviour between native and remote stores.

  The behaviour should however still be transactional (within the batch) because
  it should occur within a single SPARQL update request."
  [backend graph-uri batch-size]
  (with-open [conn (proto/->repo-connection backend)]
    (mgmt/delete-graph-batched! conn graph-uri batch-size)))
