(ns drafter.backend.protocols)

(defprotocol SparqlExecutor
  (prepare-query [this sparql-string restrictions])
  (get-query-type [this prepared-query])
  (negotiate-result-writer [this prepared-query media-type])
  (create-query-executor [this writer pquery]))

(defprotocol SparqlUpdateExecutor
  (execute-update [this update-query restrictions]))
