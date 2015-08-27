(ns drafter.backend.sesame.common
  (:require [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add statements]]
            [clojure.tools.logging :as log]
            [drafter.backend.protocols :as backend]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.backend.sesame.common.draft-management :as mgmt]
            [drafter.backend.sesame.common.draft-api :as api]
            [drafter.backend.sesame.common.batching :as batching]
            [grafter.rdf.protocols :as proto]
            [drafter.backend.sesame.common.sparql-execution :as sparql]))

(defn- get-repo [this] (:repo this))

;;SPARQL execution
(def default-sparql-query-impl
  {:prepare-query sparql/prepare-query
   :get-query-type sparql/get-query-type
   :negotiate-result-writer sparql/negotiate-result-writer
   :create-query-executor sparql/create-query-executor})

(def default-query-rewritable-impl
  {:create-rewriter sparql/->RewritingSesameSparqlExecutor})

(def default-sparql-update-impl
  {:execute-update sparql/execute-update})

;;stoppable
(def default-stoppable-impl
  {:stop (comp repo/shutdown get-repo)})

;;TODO: move to backend.common
(defn- migrate-graphs-to-live-job [backend graphs]
  (jobs/make-job :exclusive-write [job]
                 (backend/migrate-graphs-to-live! backend graphs)
                 (jobs/job-succeeded! job)))

;;draft management
(def default-draft-management-impl
  {:append-data-batch! mgmt/append-data-batch
   :append-graph-metadata! mgmt/append-graph-metadata
   :get-all-drafts mgmt/get-all-drafts
   :get-live-graph-for-draft mgmt/get-live-graph-for-draft})

(def default-sesame-batch-operations-impl
  {:delete-graph-batch! batching/delete-graph-batch!})

;;draft API
(def default-api-operations-impl
  {:new-draft-job api/new-draft-job
   :append-data-to-graph-job api/append-data-to-graph-job
   :copy-from-live-graph-job api/copy-from-live-graph-job
   :migrate-graphs-to-live-job migrate-graphs-to-live-job
   :delete-metadata-job api/delete-metadata-job
   :update-metadata-job api/create-update-metadata-job
   :delete-graph-job api/delete-graph-job})

;;Grafter SPARQL protocols

;;ITripleReadable
(def default-triple-readable-impl
  {:to-statements (fn [this options]
                    (proto/to-statements (get-repo this) options))})

;;ISPARQLable
(def default-sparqlable-impl
  {:query-dataset (fn [this sparql-string model]
                    (proto/query-dataset (get-repo this) sparql-string model))})

(def default-isparql-updatable-impl
  {:update! (fn [this sparql-string]
              (proto/update! (get-repo this) sparql-string))})

;;ITripleWritable
(defn- add-statement-impl
  ([this statement] (proto/add-statement (get-repo this) statement))
  ([this graph statement] (proto/add-statement (get-repo this) graph statement)))

(defn- add-impl
  ([this triples] (proto/add (get-repo this) triples))
  ([this graph triples] (proto/add (get-repo this) graph triples))
  ([this graph format triple-stream] (proto/add (get-repo this) format triple-stream))
  ([this graph base-uri format triple-stream] (proto/add (get-repo this) format triple-stream)))

(def default-triple-writeable-impl
  {:add-statement add-statement-impl
   :add add-impl})
