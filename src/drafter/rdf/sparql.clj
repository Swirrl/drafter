(ns drafter.rdf.sparql
  (:require [clojure.tools.logging :as log]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter.rdf.protocols :as pr]
            [grafter.rdf.repository :as repo]))

(defn query-eager-seq
  "Executes a SPARQL query which returns a sequence of results and
  ensures it is eagerly consumed before being returned. The underlying
  TCP connection is not released until all results have been iterated
  over so this prevents holding connections open longer than
  necessary."
  [repo query-string]
  (with-open [conn (repo/->connection repo)]
    (doall (repo/query conn query-string))))

(defn update! [repo update-string]
  (log/info "Running update: " update-string)
  (pr/update! repo update-string))
