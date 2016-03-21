(ns drafter.backend.sesame.common.draft-management
  (:require [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add]]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.backend.sesame.common.protocols :refer :all]))

(defn append-data-batch [backend graph-uri triple-batch]
  (with-open [conn (->repo-connection backend)]
    (repo/with-transaction conn
      (add conn graph-uri triple-batch))))

(defn append-metadata-to-graphs! [backend graph-uris metadata]
  (let [repo (->sesame-repo backend)]
    ;;TODO: Update in transaction?
    (doseq [graph-uri graph-uris
            [meta-name value] metadata]
      (mgmt/upsert-single-object! repo graph-uri meta-name value))))

(defn get-live-graph-for-draft [backend draft-graph-uri]
  (with-open [conn (->repo-connection backend)]
    (mgmt/get-live-graph-for-draft conn draft-graph-uri)))
