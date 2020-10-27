(ns drafter.backend.draftset.operations.publish
  (:require [drafter.backend.draftset.graphs :as graphs]
            [drafter.feature.modified-times :as modified-times]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.rdf.drafter-ontology :refer [modified-times-graph-uri]]
            [drafter.backend.draftset.draft-management :as mgmt]
            [clojure.java.io :as io]
            [drafter.rdf.sparql :as sparql]
            [drafter.time :as time]))

(defn- get-user-draft-graphs
  "Returns the draft graphs for the user graphs within the draftset graph mapping"
  [graph-manager live->draft]
  (keep (fn [[live-graph draft-graph]]
          (when (graphs/user-graph? graph-manager live-graph)
            draft-graph))
        live->draft))

(defn- publish-draftset-graphs! [backend graph-manager draftset-ref clock]
  (let [live->draft (dsops/get-draftset-graph-mapping backend draftset-ref)
        user-drafts (get-user-draft-graphs graph-manager live->draft)]
    (mgmt/migrate-graphs-to-live! backend user-drafts clock)
    (modified-times/publish-modifications-graph backend live->draft (time/now clock))))

(defn- update-public-endpoint-modified-at-query []
  (slurp (io/resource "drafter/backend/draftset/operations/publish/update-public-endpoint-modified-at.sparql")))

(defn update-public-endpoint-modified-at!
  "Updates the modified time of the public endpoint to the current time"
  [backend]
  (let [q (update-public-endpoint-modified-at-query)]
    (sparql/update! backend q)))

(defn publish-draftset!
  "Publishes the referenced draftset at the time returned by clock"
  [{:keys [backend graph-manager clock] :as manager} draftset-ref]
  (publish-draftset-graphs! backend graph-manager draftset-ref clock)
  (update-public-endpoint-modified-at! backend)
  (dsops/delete-draftset-statements! backend draftset-ref))
