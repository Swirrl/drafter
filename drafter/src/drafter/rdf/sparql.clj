(ns drafter.rdf.sparql
  (:require [clojure.tools.logging :as log]
            [cognician.dogstatsd :as datadog]
            [drafter.rdf.drafter-ontology :refer :all]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.repository :as repo]
            [medley.core :as med]))

(defn eager-query
  "Executes a SPARQL query which returns a sequence of results and
  ensures it is eagerly consumed before being returned. The underlying
  TCP connection is not released until all results have been iterated
  over so this prevents holding connections open longer than
  necessary."

  ([repo query-str]
   (eager-query repo query-str {}))
  ([repo query-str opts]
   (with-open [conn (repo/->connection repo)]
     (let [res (med/mapply repo/query conn query-str opts)]
       (if (seq? res)
         (doall res)
         res)))))

(defn select-1
  "Executes the given SELECT query and returns the single expected result if present.
   Raises an exception if the query returns more than one solution."
  [repo query]
  (let [bindings (with-open [conn (repo/->connection repo)]
                   (vec (repo/query conn query)))]
    (case (count bindings)
      0 nil
      1 (first bindings)
      (throw (ex-info "Query returned multiple results - expected 0 or 1" {:query query
                                                                           :results bindings})))))

(defn update! [repo update-string]
  (log/info "Running update: " update-string)
  (datadog/measure!
   "drafter.sparql.update.time" {}
   (with-open [conn (repo/->connection repo)]
     (pr/update! conn update-string))))

(defn add
  ([db triples]
   (with-open [conn (repo/->connection db)]
     (pr/add conn triples)))
  ([db graph-uri triples]
   (with-open [conn (repo/->connection db)]
     (pr/add conn graph-uri triples))))
