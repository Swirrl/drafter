(ns drafter.backend.live
  "A thin wrapper over a Repository/Connection that implements a graph
  restriction, hiding all but the set of live (ManagedGraph)'s."
  (:require [drafter.backend.draftset.draft-management :as mgmt]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.io :as rio]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [drafter.backend.common :as bprot])
  (:import java.io.Closeable
           [org.apache.jena.query QueryFactory Syntax]
           org.eclipse.rdf4j.model.impl.URIImpl
           org.eclipse.rdf4j.model.Resource))

(defn- build-restricted-connection [{:keys [inner restriction]}]
  (let [stasher-conn (repo/->connection inner)]
    (reify
      repo/IPrepareQuery
      (prepare-query* [this sparql-string dataset]
        (let [query (QueryFactory/create sparql-string Syntax/syntaxSPARQL_11)
              user-restriction (some-> dataset bprot/dataset->restriction)
              query-restriction (bprot/query-dataset-restriction query)]
          (-> stasher-conn
              (bprot/prep-and-validate-query sparql-string)
              (bprot/restrict-query user-restriction
                                    query-restriction
                                    restriction))))

      ;; Currently restricted connections only support querying...
      pr/ISPARQLable
      (query-dataset [this sparql-string dataset]
        (pr/query-dataset this sparql-string dataset {}))
      (query-dataset [this sparql-string dataset opts]
        (let [pquery (repo/prepare-query this sparql-string dataset opts)]
          (repo/evaluate pquery)))

      Closeable
      (close [this]
        (.close stasher-conn))

      ;; For completeness... a to-statements implementation that
      ;; enforces the graph restriction.
      pr/ITripleReadable
      (pr/to-statements [this {:keys [:grafter.repository/infer] :or {infer true}}]
        (let [f (fn next-item [i]
                  (when (.hasNext i)
                    (let [v (.next i)]
                      (lazy-seq (cons (rio/backend-quad->grafter-quad v) (next-item i))))))]
          (let [iter (.getStatements stasher-conn nil nil nil infer (into-array Resource (map #(URIImpl. (str %)) (restriction))))]
            (f iter)))))))

(defrecord RestrictedExecutor [inner restriction]
  repo/ToConnection
  (->connection [this]
    (build-restricted-connection this)))

(defn live-endpoint-with-stasher
  "Creates a backend restricted to the live graphs."
  [{:keys [repo]}]
  (->RestrictedExecutor repo (partial mgmt/live-graphs repo)))

(defmethod ig/init-key ::endpoint [_ opts]
  (live-endpoint-with-stasher opts))
