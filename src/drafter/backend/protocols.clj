(ns drafter.backend.protocols
  (:require [grafter.rdf.repository :as repo]))

(defprotocol SparqlExecutor
  (prepare-query [this sparql-string]))

(defprotocol ToRepository
  (->sesame-repo [this]
    "Gets the sesame repository for this backend"))

(defn stop-backend [backend]
  (repo/shutdown (->sesame-repo backend)))

(defn ->repo-connection
  "Opens a connection to the underlying Sesame repository for the
  given backend."
  [backend]
  (repo/->connection (->sesame-repo backend)))
