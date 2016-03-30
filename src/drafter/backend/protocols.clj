(ns drafter.backend.protocols)

(defprotocol SparqlExecutor
  (all-quads-query [this])
  (prepare-query [this sparql-string])
  (get-query-type [this prepared-query])
  (create-query-executor [this result-format pquery]))

(defprotocol StatementDeletion
  (delete-quads-from-draftset-job [this serialised rdf-format draftset-ref])
  (delete-triples-from-draftset-job [this serialised rdf-format draftset-ref graph]))

(defprotocol QueryRewritable
  (create-rewriter [this live->draft union-with-live?])
  (create-restricted [this graph-restriction]))

(defprotocol SparqlUpdateExecutor
  (execute-update [this update-query]))

;; NOTE: We should eventually replace this when we migrate to using Stuart
;; Sierra's Component.
(defprotocol Stoppable
  (stop [this]))
