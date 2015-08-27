(ns drafter.backend.sesame.common.draft-management
  (:require [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add]]
            [drafter.rdf.draft-management :as mgmt]))

(def ^:private get-repo :repo)

(defn append-data-batch [backend graph-uri triple-batch]
  (let [repo (get-repo backend)]
    (with-open [conn (repo/->connection repo)]
      (repo/with-transaction conn
        (add conn graph-uri triple-batch)))))

(defn append-graph-metadata [backend graph-uri metadata]
  (let [repo (get-repo backend)]
    ;;TODO: Update in transaction?
    (doseq [[meta-name value] metadata]
      (mgmt/upsert-single-object! repo graph-uri meta-name value))))

(defn get-all-drafts [backend]
  (with-open [conn (repo/->connection (get-repo backend))]
    (mgmt/query-all-drafts conn)))

(defn get-live-graph-for-draft [backend draft-graph-uri]
  (with-open [conn (repo/->connection (get-repo backend))]
    (mgmt/get-live-graph-for-draft conn draft-graph-uri)))
