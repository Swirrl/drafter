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

(defprotocol DraftManagement
  (append-data-batch! [this draft-graph-uri triple-batch]
    "Appends a sequence of triples to the given draft graph.")

  (append-graph-metadata! [this draft-graph-uri metadata]
    "Takes a hash-map of metadata key/value pairs and adds them as
  metadata to the graphs state graph, converting keys into drafter
  URIs as necessary.  Assumes all values are strings."))

(defprotocol Stoppable
  (stop [this]))

(defprotocol ApiOperations
  (new-draft-job [this live-graph-uri params])
  (append-data-to-graph-job [this graph data rdf-format metadata])
  (copy-from-live-graph-job [this draft-graph-uri])
  (migrate-graphs-to-live-job [this graphs])
  (delete-metadata-job [this graphs meta-keys])
  (update-metadata-job [this graphs metadata])
  (delete-graph-job [this graph contents-only?]
    "Deletes graph contents as per batch size in order to avoid
   blocking writes with a lock. Finally the graph itself will be
   deleted unless contents-only? is true"))
