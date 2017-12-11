(ns drafter.backend.live
  "A thin wrapper over a Repository/Connection that implements a graph
  restriction, hiding all but the set of live (ManagedGraph)'s."
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.common :as bprot :refer [->sesame-repo]]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.spec :as bs]
            [grafter.rdf.protocols :as proto]
            [grafter.rdf4j.io :as rio]
            [grafter.rdf4j.repository :as repo]
            [integrant.core :as ig])
  (:import java.io.Closeable
           org.eclipse.rdf4j.model.impl.URIImpl
           org.eclipse.rdf4j.model.Resource))

(defn- build-restricted-connection [{:keys [inner restriction]}]
  (let [stasher-conn (repo/->connection inner)]
    (reify
      repo/IPrepareQuery
      (repo/prepare-query* [this sparql-string _]
        (let [pquery (-> (bprot/prep-and-validate-query stasher-conn sparql-string)
                         (bprot/apply-restriction restriction))]
          pquery))
      
      ;; Currently restricted connections only support querying...
      proto/ISPARQLable
      (proto/query-dataset [this sparql-string dataset]
        (let [pquery (repo/prepare-query* this sparql-string dataset)]
          (repo/evaluate pquery)))
      
      Closeable
      (close [this]
        (.close stasher-conn))

      ;; For completeness... a to-statements implementation that
      ;; enforces the graph restriction.
      proto/ITripleReadable
      (pr/to-statements [this {:keys [:grafter.repository/infer] :or {infer true}}]
        (let [f (fn next-item [i]
                  (when (.hasNext i)
                    (let [v (.next i)]
                      (lazy-seq (cons (rio/backend-quad->grafter-quad v) (next-item i))))))]
          (let [iter (.getStatements stasher-conn nil nil nil infer (into-array Resource (map #(URIImpl. (str %)) (restriction))))]
            (f iter)))))))

(defrecord RestrictedExecutor [inner restriction]
  ;; TODO Think we should get rid of this...
  #_bprot/SparqlExecutor
  #_(prepare-query [this query-string]
    (let [pquery (bprot/prep-and-validate-query inner query-string)]
      (bprot/apply-restriction pquery restriction)))

  bprot/ToRepository
  (->sesame-repo [_] (->sesame-repo inner))

  repo/ToConnection
  (repo/->connection [this]
    (build-restricted-connection this)))

(defn live-endpoint-with-stasher
  "Creates a backend restricted to the live graphs."
  [{:keys [uncached-repo stasher-repo]}]
  ;; TODO: remove need for uncached repo. Doing so will require
  ;; state-graph inception, i.e. storing data on the state graph in
  ;; the stategraph.
  (->RestrictedExecutor stasher-repo (partial mgmt/live-graphs uncached-repo)))

(defmethod ig/pre-init-spec ::endpoint [_]
  (s/keys :req-un [::bs/uncached-repo ::bs/stasher-repo]))

(defmethod ig/init-key ::endpoint [_ opts]
  (live-endpoint-with-stasher opts))
