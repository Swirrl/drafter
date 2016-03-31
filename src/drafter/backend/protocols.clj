(ns drafter.backend.protocols)

(defprotocol SparqlExecutor
  (prepare-query [this sparql-string])
  (get-query-type [this prepared-query])
  (create-query-executor [this result-format pquery]))

(defprotocol SparqlUpdateExecutor
  (execute-update [this update-query]))

;; NOTE: We should eventually replace this when we migrate to using Stuart
;; Sierra's Component.
(defprotocol Stoppable
  (stop [this]))
