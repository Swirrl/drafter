(ns drafter.backend.protocols)

(defprotocol SparqlExecutor
  (prepare-query [this sparql-string]))

(defprotocol SparqlUpdateExecutor
  (execute-update [this update-query]))

;; NOTE: We should eventually replace this when we migrate to using Stuart
;; Sierra's Component.
(defprotocol Stoppable
  (stop [this]))
