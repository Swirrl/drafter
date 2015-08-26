(ns drafter.backend.sesame-native
  (:require [environ.core :refer [env]]
            [clojure.tools.logging :as log]
            [drafter.backend.protocols :refer :all]
            [drafter.backend.sesame-common :refer :all]
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

(defrecord SesameNativeBackend [repo])

(extend SesameNativeBackend
  repo/ToConnection default-to-connection-impl
  proto/ITripleReadable default-triple-readable-impl
  proto/ITripleWriteable default-triple-writeable-impl
  proto/ISPARQLable default-sparqlable-impl
  proto/ISPARQLUpdateable default-isparql-updatable-impl
  SparqlExecutor default-sparql-query-impl
  QueryRewritable default-query-rewritable-impl
  SparqlUpdateExecutor default-sparql-update-impl
  ApiOperations default-api-operations-impl
  DraftManagement default-draft-management-impl
  Stoppable default-stoppable-impl

  SesameBatchOperations default-sesame-batch-operations-impl)

(def get-backend-for-repo ->SesameNativeBackend)

(defn get-native-backend []
  (get-backend-for-repo (get-repo env)))

(defn reindex
  "Reindex the database according to the DRAFTER_INDEXES set at
  DRAFTER_REPO_PATH in the environment.  If no environment variables
  are set for these values the defaults are used."
  []
  (let [{:keys [indexes repo-path]} (get-repo-config env)]
    (log/info "Reindexing database at" repo-path " with indexes" indexes)
    (get-repo env)
    (log/info "Reindexing finished")))
