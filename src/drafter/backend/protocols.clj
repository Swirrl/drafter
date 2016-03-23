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
