(ns drafter.backend.sesame.common.protocols
  (:require [grafter.rdf.repository :refer [->connection]]))

(defprotocol ToRepository
  (->sesame-repo [this]
    "Gets the sesame repository for this backend"))

(defn ->repo-connection [backend]
  "Opens a connection to the underlying Sesame repository for the
  given backend."
  (->connection (->sesame-repo backend)))
