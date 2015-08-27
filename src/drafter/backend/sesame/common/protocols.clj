(ns drafter.backend.sesame.common.protocols)

(defprotocol SesameBatchOperations
  (delete-graph-batch! [this graph-uri batch-size]
    "Deletes batch-size triples from the given graph uri. Most sesame
    backends delete graphs in batches rather than DROPping the target
    graph since it may be slow. The default delete graph job deletes
    the source graph in batches so backends wishing to use the default
    implementation should also implement this protocol."))

(defprotocol ToRepository
  (->sesame-repo [this]
    "Gets the sesame repository for this backend"))
