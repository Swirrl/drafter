(ns drafter.backend.sesame.remote.impl
  "Contains the default implmentation of some backend protocols for
  backends which use a Sesame SPARQL client to access the graph
  store."
  (:require [drafter.backend.sesame.remote.sparql-execution :as sparqlexec]))

(def sparql-update-executor-impl {:execute-update sparqlexec/execute-update})
