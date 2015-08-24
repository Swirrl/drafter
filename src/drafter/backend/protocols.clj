(ns drafter.backend.protocols)

(defprotocol SparqlExecutor
  (prepare-query [this sparql-string restrictions])
  (get-query-type [this prepared-query])
  (negotiate-result-writer [this prepared-query media-type])
  (create-query-executor [this writer pquery]))

(defprotocol QueryRewritable
  (create-rewriter [this live->draft]))

(defprotocol SparqlUpdateExecutor
  (execute-update [this update-query restrictions]))

(defprotocol DraftManagement)

(defprotocol Stoppable
  (stop [this]))

(defprotocol ApiOperations
  (new-draft-job [this live-graph-uri params])
  (append-data-to-graph-job [this graph data rdf-format metadata])
  (migrate-graphs-to-live-job [this graphs])
  (delete-metadata-job [this graphs meta-keys])
  (update-metadata-job [this graphs metadata])
  (delete-graph-job [this graph contents-only?]
    "Deletes graph contents as per batch size in order to avoid
   blocking writes with a lock. Finally the graph itself will be
   deleted unless contents-only? is true"))
