(ns drafter.backend.sesame.native.repository
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [grafter.rdf.repository :as repo]))

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

(defn get-repository [env-map]
  (let [{:keys [indexes repo-path]} (get-repo-config env)]
    (get-repo-at repo-path indexes)))

(defn reindex
  "Reindex the database according to the DRAFTER_INDEXES set at
  DRAFTER_REPO_PATH in the environment.  If no environment variables
  are set for these values the defaults are used."
  []
  (let [{:keys [indexes repo-path]} (get-repo-config env)]
    (log/info "Reindexing database at" repo-path " with indexes" indexes)
    (get-repository env)
    (log/info "Reindexing finished")))
