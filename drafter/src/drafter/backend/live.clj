(ns drafter.backend.live
  "A thin wrapper over a Repository/Connection that implements a graph
  restriction, hiding all but the set of live (ManagedGraph)'s."
  (:require [drafter.backend.draftset.draft-management :as mgmt]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [drafter.rdf.sesame :as ses]
            [drafter.rdf.dataset :as dataset]
            [drafter.backend.draftset.arq :as arq])
  (:import [java.io Closeable]
           [org.eclipse.rdf4j.query Query]))

(defn- build-restricted-connection [inner restriction]
  (let [stasher-conn (repo/->connection inner)]
    (reify
      repo/IPrepareQuery
      (prepare-query* [_this sparql-string rdf4j-dataset]
        (let [query (arq/sparql-string->arq-query sparql-string)
              query-dataset (dataset/->dataset query)
              user-dataset (dataset/->dataset rdf4j-dataset)
              live-graphs (restriction)
              ^Query pquery (repo/prepare-query stasher-conn sparql-string)
              restricted-dataset (dataset/get-query-dataset query-dataset user-dataset live-graphs)]
          (.setDataset pquery restricted-dataset)
          pquery))

      ;; Currently restricted connections only support querying
      pr/ISPARQLable
      (query-dataset [this sparql-string dataset]
        (pr/query-dataset this sparql-string dataset {}))
      (query-dataset [this sparql-string dataset opts]
        (let [pquery (repo/prepare-query this sparql-string dataset opts)]
          (repo/evaluate pquery)))

      Closeable
      (close [_this]
        (.close stasher-conn))

      pr/ITripleReadable
      (to-statements [_this {:keys [:grafter.repository/infer] :or {infer true}}]
        (let [live-graphs (restriction)]
          (ses/get-statements stasher-conn infer live-graphs))))))

(defrecord RestrictedExecutor [inner restriction]
  repo/ToConnection
  (->connection [_this]
    (build-restricted-connection inner restriction)))

(defn live-endpoint-with-stasher
  "Creates a backend restricted to the live graphs."
  [repo]
  (->RestrictedExecutor repo (partial mgmt/live-graphs repo)))

(defmethod ig/init-key ::endpoint [_ {:keys [repo] :as opts}]
  (live-endpoint-with-stasher repo))
