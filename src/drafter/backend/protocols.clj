(ns drafter.backend.protocols
  (:require [drafter.util :as util]))

(defprotocol SparqlExecutor
  (all-quads-query [this restrictions])
  (prepare-query [this sparql-string restrictions])
  (get-query-type [this prepared-query])
  (negotiate-result-writer [this prepared-query media-type])
  (create-query-executor [this writer pquery]))

(defprotocol StatementDeletion
  (delete-quads-from-draftset-job [this serialised rdf-format draftset-ref])
  (delete-triples-from-draftset-job [this serialised rdf-format draftset-ref graph]))

(defprotocol QueryRewritable
  (create-rewriter [this live->draft]))

(defprotocol SparqlUpdateExecutor
  (execute-update [this update-query restrictions]))

;; NOTE: We should eventually replace this when we migrate to using Stuart
;; Sierra's Component.
(defprotocol Stoppable
  (stop [this]))

(defprotocol ApiOperations
  (append-data-to-draftset-job [this draftset-ref tempfile rdf-format]
    "Return a job that appends RDF data to the given draftset")
  (append-triples-to-draftset-job [this draftset-ref data rdf-format graph]
    "Returns a job that appends triples to the given draftset")
  (publish-draftset-job [this draftset-ref]
    "Return a job that publishes the graphs in a draftset to live and
    then deletes the draftset.")
  (delete-draftset! [this draftset-ref]
    "Deletes a draftset and all of its constituent graphs")
  (migrate-graphs-to-live-job [this graphs]
    "Return a job to migrate the supplied set of draft graphs to live."))
