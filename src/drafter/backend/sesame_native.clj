(ns drafter.backend.sesame-native
  (:require [environ.core :refer [env]]
            [clojure.tools.logging :as log]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame-common :refer :all]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.draft-management :as mgmt]
            [grafter.rdf :refer [add]]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :as proto]))

;; http://sw.deri.org/2005/02/dexa/yars.pdf - see table on p5 for full coverage of indexes.
;; (but we have to specify 4 char strings, so in some cases last chars don't matter
(def default-indexes "spoc,pocs,ocsp,cspo,cpos,oscp")

(def default-repo-path "drafter-db")

(defn- get-repo-at [repo-path indexes]
  (let [repo (repo/repo (repo/native-store repo-path indexes))]
    (log/info "Initialised repo" repo-path)
    repo))

(defn- get-repo-config [env-map]
  {:indexes (get env-map :drafter-indexes default-indexes)
   :repo-path (get env-map :drafter-repo-path default-repo-path)})

(defn- get-repo [env-map]
  (let [{:keys [indexes repo-path]} (get-repo-config env)]
    (get-repo-at repo-path indexes)))

(defn- migrate-graph-to-live!
  "Moves the triples from the draft graph to the draft graphs live destination."
  [db draft-graph-uri]

  (if-let [live-graph-uri (mgmt/lookup-live-graph db draft-graph-uri)]
    (do
      ;;DELETE the target (live) graph and copy the draft to it
      ;;TODO: Use MOVE query?
      (mgmt/delete-graph-contents! db live-graph-uri)

      (let [contents (repo/query db
                            (str "CONSTRUCT { ?s ?p ?o } WHERE
                                 { GRAPH <" draft-graph-uri "> { ?s ?p ?o } }"))

            ;;If the source (draft) graph is empty then the migration
            ;;is a deletion. If it is the only draft of the live graph
            ;;then all references to the live graph are being removed
            ;;from the data. In this case the reference to the live
            ;;graph should be removed from the state graph. Note this
            ;;case and use it when cleaning up the state graph below.
            is-only-draft? (not (mgmt/has-more-than-one-draft? db live-graph-uri))]

        ;;if the source (draft) graph has data then copy it to the live graph and
        ;;make it public.
        (if (not (empty? contents))
          (do
            (add db live-graph-uri contents)
            (mgmt/set-isPublic! db live-graph-uri true)))

        ;;delete draft data
        (mgmt/delete-graph-contents! db draft-graph-uri)

        ;;NOTE: At this point all the live and draft graph data has
        ;;been updated: the live graph contents match those of the
        ;;published draft, and the draft data has been deleted.

        ;;Clean up the state graph - all references to the draft graph should always be removed.
        ;;The live graph should be removed if the draft was empty (operation was a deletion) and
        ;;it was the only draft of the live graph

        ;;WARNING: Draft graph state must be deleted before the live graph state!
        ;;DELETE query depends on the existence of the live->draft connection in the state
        ;;graph
        (mgmt/delete-draft-graph-state! db draft-graph-uri)

        (if (and is-only-draft? (empty? contents))
          (mgmt/delete-live-graph-from-state! db live-graph-uri)))
      
      (log/info (str "Migrated graph: " draft-graph-uri " to live graph: " live-graph-uri)))

    (throw (ex-info (str "Could not find the live graph associated with graph " draft-graph-uri)
                    {:error :graph-not-found}))))

(defn- migrate-graphs-to-live!-impl [backend graphs]
  (log/info "Starting make-live for graphs " graphs)
  (with-open [conn (repo/->connection (:repo backend))]
    (repo/with-transaction conn
      (doseq [g graphs]
        (migrate-graph-to-live! conn g))))
  (log/info "Make-live for graphs " graphs " done"))

(defrecord SesameNativeBackend [repo])

(extend SesameNativeBackend
  proto/ITripleReadable default-triple-readable-impl
  proto/ITripleWriteable default-triple-writeable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor default-sparql-update-impl
  ApiOperations default-api-operations-impl
  DraftManagement (assoc default-draft-management-impl :migrate-graphs-to-live! migrate-graphs-to-live!-impl)
  Stoppable default-stoppable-impl

  SesameBatchOperations default-sesame-batch-operations-impl)

(def get-backend-for-repo ->SesameNativeBackend)

(defn get-native-backend [env-map]
  (get-backend-for-repo (get-repo env-map)))

(defn reindex
  "Reindex the database according to the DRAFTER_INDEXES set at
  DRAFTER_REPO_PATH in the environment.  If no environment variables
  are set for these values the defaults are used."
  []
  (let [{:keys [indexes repo-path]} (get-repo-config env)]
    (log/info "Reindexing database at" repo-path " with indexes" indexes)
    (get-repo env)
    (log/info "Reindexing finished")))
