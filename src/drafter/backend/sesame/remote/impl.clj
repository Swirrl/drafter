(ns drafter.backend.sesame.remote.impl
  "Contains the default implmentation of some backend protocols for
  backends which use a Sesame SPARQL client to access the graph
  store."
  (:require [drafter.backend.sesame.common :as common-impl]
            [drafter.backend.sesame.remote.draft-management :as sparqlmgmt]
            [drafter.backend.sesame.remote.sparql-execution :as sparqlexec]))

(def sparql-update-executor-impl {:execute-update sparqlexec/execute-update})

(def draft-management-impl
  (assoc common-impl/default-draft-management-impl
    :append-data-batch! sparqlmgmt/append-data-batch
    :append-metadata-to-graphs! sparqlmgmt/append-metadata-to-graphs!
    :migrate-graphs-to-live! sparqlmgmt/migrate-graphs-to-live!))
