(ns drafter.backend.protocols
  (:require [grafter.rdf.repository :refer [->connection shutdown]]))

(defprotocol SparqlExecutor
  (prepare-query [this sparql-string]))

(defprotocol SparqlUpdateExecutor
  (execute-update [this update-query]))

(defprotocol ToRepository
  (->sesame-repo [this]
    "Gets the sesame repository for this backend"))

(defn stop-backend [backend]
  (shutdown (->sesame-repo backend)))

(defn ->repo-connection [backend]
  "Opens a connection to the underlying Sesame repository for the
  given backend."
  (->connection (->sesame-repo backend)))
