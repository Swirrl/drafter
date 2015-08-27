(ns drafter.backend.sesame.common.batching
  (:require [grafter.rdf.repository :as repo]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.backend.sesame.common.protocols :as proto]))

(defn delete-graph-batch!
  "Default implementation for deleting a batch statements from a graph"
  [backend graph-uri batch-size]
  (with-open [conn (repo/->connection (proto/->sesame-repo backend))]
    (repo/with-transaction conn
      (mgmt/delete-graph-batched! conn graph-uri batch-size))))
