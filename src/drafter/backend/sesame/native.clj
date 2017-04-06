(ns drafter.backend.sesame.native
  (:require [drafter.backend.repository]
            [clojure.tools.logging :as log]
            [grafter.rdf.repository :as repo]))

(defn- get-repo-at [repo-path indexes]
  (let [repo (repo/repo (repo/native-store repo-path indexes))]
    (log/info "Initialised repo" repo-path)
    repo))

(defn get-repository [{:keys [sesame-store indexes repo-path] :as config}]
  (condp = sesame-store
    "native-store" (get-repo-at repo-path indexes)
    "memory-store" (repo/repo)))

(defn get-backend [config]
  (get-repository config))
