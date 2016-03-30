(ns drafter.backend.protocols)

(defprotocol SparqlExecutor
  (all-quads-query [this])
  (prepare-query [this sparql-string])
  (get-query-type [this prepared-query])
  (create-query-executor [this result-format pquery]))

(defprotocol QueryRewritable
  (create-rewriter [this live->draft union-with-live?])
  (create-restricted [this graph-restriction]))

(defprotocol SparqlUpdateExecutor
  (execute-update [this update-query]))

;; NOTE: We should eventually replace this when we migrate to using Stuart
;; Sierra's Component.
(defprotocol Stoppable
  (stop [this]))
