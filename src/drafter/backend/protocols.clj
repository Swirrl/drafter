(ns drafter.backend.protocols
  (:require [grafter.rdf.repository :refer [->connection]]))

(defprotocol SparqlExecutor
  (prepare-query [this sparql-string]))

(defprotocol SparqlUpdateExecutor
  (execute-update [this update-query]))

(defprotocol ToRepository
  (->sesame-repo [this]
    "Gets the sesame repository for this backend"))

;; NOTE: We should eventually replace this when we migrate to using Stuart
;; Sierra's Component.
(defprotocol Stoppable
  (stop [this]))

(defn ->repo-connection [backend]
  "Opens a connection to the underlying Sesame repository for the
  given backend."
  (->connection (->sesame-repo backend)))
