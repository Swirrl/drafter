(ns drafter.backend.draftset
  (:require [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.backend.draftset.operations :as dsmgmt]
            [drafter.backend.draftset.rewrite-query :refer [rewrite-sparql-string]]
            [drafter.backend.draftset.rewrite-result :refer [rewriting-query] :as rer]
            [grafter-2.rdf.protocols :as proto]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [clojure.set :as set]
            [drafter.rdf.sesame :as ses]
            [drafter.backend.draftset.arq :as arq]
            [drafter.rdf.dataset :as dataset])
  (:import java.io.Closeable
           [org.eclipse.rdf4j.repository RepositoryConnection]
           [org.eclipse.rdf4j.query Query]))

(defn- raw-rewrite-query
  "Rewrites a SPARQL query according to the draftset graph mapping and returns an RDF4j prepared query along
   with the dataset specified in the query using FROM and FROM NAMED. The dataset is specified in terms of the
   live graphs in the query not the corresponding draft graphs."
  [conn live->draft sparql-string]
  (let [query (arq/sparql-string->arq-query sparql-string)
        query-dataset (dataset/->dataset query)
        rewritten-query (rewrite-sparql-string live->draft sparql-string)
        pq (repo/prepare-query conn rewritten-query)
        rewriting-query (rer/rewriting-query pq live->draft)]
    {:prepared-query rewriting-query :query-dataset query-dataset}))

(defn- visible-mananged-graphs
  "Returns the set of managed graphs visible within the draftset. Includes all live graphs if union-with-live?
  is true otherwise just returns the managed graphs with a corresponding draft in the draftset."
  [repo live->draft union-with-live?]
  (let [non-draft-live-graphs (if union-with-live?
                                (set (mgmt/live-graphs repo))
                                #{})]
    (set/union (set (keys live->draft)) non-draft-live-graphs)))

(defn- prepare-rewrite-query
  "Creates a prepared RDF4j query against the draftset with the given live to draft graph mapping.
   Rewrites the query according to the mapping and sets the dataset according to either the query
   or the optional user dataset provided."
  [repo conn live->draft sparql-string union-with-live? rdf4j-conn-dataset]
  (let [{:keys [^Query prepared-query query-dataset]} (raw-rewrite-query conn live->draft sparql-string)
        user-dataset (dataset/->dataset rdf4j-conn-dataset)
        visible-graphs (visible-mananged-graphs repo live->draft union-with-live?)
        rdf4j-dataset (dataset/get-query-dataset query-dataset user-dataset visible-graphs)]
    (.setDataset prepared-query rdf4j-dataset)
    prepared-query))

(defn- build-draftset-connection [{:keys [repo live->draft union-with-live?]}]
  (let [^RepositoryConnection conn (repo/->connection repo)]
    (reify
      repo/IPrepareQuery
      (prepare-query* [_this sparql-string dataset]
        (prepare-rewrite-query repo
                               conn
                               live->draft
                               sparql-string
                               union-with-live?
                               dataset))

      proto/ISPARQLable
      (query-dataset [this sparql-string model]
        (proto/query-dataset this sparql-string model {}))
      (query-dataset [this sparql-string model _opts]
        (let [pquery (repo/prepare-query* this sparql-string model)]
          (repo/evaluate pquery)))

      Closeable
      (close [_this]
        (.close conn))

      proto/ITripleReadable
      (to-statements [_this {:keys [:grafter.repository/infer] :or {infer true}}]
        (let [draft->live (set/map-invert live->draft)
              graphs (mgmt/draft-raw-graphs repo live->draft union-with-live?)
              rewrite-statement (fn [stmt] (rer/rewrite-statement draft->live stmt))]
          (map rewrite-statement (ses/get-statements conn infer graphs)))))))

(defrecord RewritingSesameSparqlExecutor [repo live->draft union-with-live?]
  repo/ToConnection
  (->connection [this]
    (build-draftset-connection this)))

(defn build-draftset-endpoint
  "Build a SPARQL queryable repo representing the draftset"
  [repo draftset-ref union-with-live?]
  (let [graph-mapping (dsmgmt/get-draftset-graph-mapping repo draftset-ref)]
    (->RewritingSesameSparqlExecutor repo graph-mapping union-with-live?)))

(defmethod ig/init-key ::endpoint [_ opts]
  opts)
