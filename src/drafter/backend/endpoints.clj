(ns drafter.backend.endpoints
  (:require [grafter.rdf.protocols :as proto]
            [drafter.backend.sesame.common.sparql-execution :refer [apply-restriction execute-restricted-update prepare-query]]
            [drafter.backend.protocols :refer [->sesame-repo] :as backend]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.rdf.rewriting.query-rewriting :refer [rewrite-sparql-string]]
            [drafter.rdf.rewriting.result-rewriting :refer [rewrite-query-results rewrite-statement]]
            [drafter.util :as util]
            [schema.core :as s])
  (:import [org.openrdf.repository Repository]
           [org.openrdf.model URI]))

(def ^:private itriple-readable-delegate
  {:to-statements (fn [this options]
                    (proto/to-statements (->sesame-repo this) options))})

(def ^:private isparqlable-delegate
  {:query-dataset (fn [this sparql-string model]
                    (proto/query-dataset (->sesame-repo this) sparql-string model))})

(def ^:private isparql-updateable-delegate
  {:update! (fn [this sparql-string]
              (proto/update! (->sesame-repo this) sparql-string))})

(defn- add-delegate
  ([this triples] (proto/add (->sesame-repo this) triples))
  ([this graph triples] (proto/add (->sesame-repo this) graph triples))
  ([this graph format triple-stream] (proto/add (->sesame-repo this) graph format triple-stream))
  ([this graph base-uri format triple-stream] (proto/add (->sesame-repo this) graph base-uri format triple-stream)))

(defn- add-statement-delegate
  ([this statement] (proto/add-statement (->sesame-repo this) statement))
  ([this graph statement] (proto/add-statement (->sesame-repo this) graph statement)))

(def ^:private itriple-writeable-delegate
  {:add add-delegate
   :add-statement add-statement-delegate})

(defrecord RestrictedExecutor [db restriction]
  backend/SparqlExecutor

  (prepare-query [this query-string]
    (let [pquery (prepare-query this query-string)]
      (apply-restriction pquery restriction)))

  backend/SparqlUpdateExecutor
  (execute-update [this update-query]
    (execute-restricted-update this update-query restriction))

  backend/ToRepository
  (->sesame-repo [_] db))

(extend RestrictedExecutor
  proto/ITripleReadable itriple-readable-delegate
  proto/ITripleWriteable itriple-writeable-delegate
  proto/ISPARQLable isparqlable-delegate
  proto/ISPARQLUpdateable isparql-updateable-delegate)

(defn create-restricted [backend restriction]
  (->RestrictedExecutor (->sesame-repo backend) restriction))

(defn- stringify-graph-mapping [live->draft]
  (util/map-all #(.stringValue %) live->draft))

(defn- get-rewritten-query-graph-restriction [db live->draft union-with-live?]
  (mgmt/graph-mapping->graph-restriction db (stringify-graph-mapping live->draft) union-with-live?))

(defn- prepare-restricted-query [backend sparql-string graph-restriction]
  (let [pquery (prepare-query backend sparql-string)]
    (apply-restriction pquery graph-restriction)))

(s/defrecord RewritingSesameSparqlExecutor [db :- Repository
                                            live->draft :- {URI URI}
                                            union-with-live? :- Boolean]
  backend/SparqlExecutor
  (prepare-query [this sparql-string]
    (let [rewritten-query-string (rewrite-sparql-string live->draft sparql-string)
          graph-restriction (get-rewritten-query-graph-restriction db live->draft union-with-live?)
          prepared-query (prepare-restricted-query this rewritten-query-string graph-restriction)]
      (rewrite-query-results prepared-query live->draft)))
  
  backend/ToRepository
  (->sesame-repo [_] db))

(extend RewritingSesameSparqlExecutor
  proto/ITripleReadable itriple-readable-delegate
  proto/ITripleWriteable itriple-writeable-delegate
  proto/ISPARQLable isparqlable-delegate
  proto/ISPARQLUpdateable isparql-updateable-delegate)

(defn draft-graph-set [backend live->draft union-with-live?]
  (->RewritingSesameSparqlExecutor (->sesame-repo backend) live->draft union-with-live?))
