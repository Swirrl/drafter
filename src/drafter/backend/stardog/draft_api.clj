(ns drafter.backend.stardog.draft-api
  (:require [drafter.rdf.draft-management :refer [update! delete-draft-state-query]]
            [swirrl-server.async.jobs :refer [create-job]]
            [taoensso.timbre :as log]
            [drafter.rdf.draft-management.jobs :as jobs]))

(defn delete-graph!
  "Deletes graph contents as per batch size in order to avoid blocking
  writes with a lock."
  [backend graph-uri contents-only? job]

  (let [drop-statement (str "DROP SILENT GRAPH <" graph-uri ">")]

    (if contents-only?
      (update! backend drop-statement)
      (update! backend (str drop-statement " ; "
                            (delete-draft-state-query graph-uri))))

    (jobs/job-succeeded! job)))

(defn delete-graph-job [this graph-uri contents-only?]
  (log/info "Starting deletion job")
  (create-job :batch-write
              (partial delete-graph! this graph-uri contents-only?)))
