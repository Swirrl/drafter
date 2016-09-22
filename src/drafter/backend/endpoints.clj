(ns drafter.backend.endpoints
  (:require [drafter.backend.protocols :as backend :refer [->sesame-repo]]
            [drafter.rdf
             [draft-management :as mgmt]
             [sesame :refer [apply-restriction execute-restricted-update prepare-query]]]
            [drafter.rdf.rewriting
             [query-rewriting :refer [rewrite-sparql-string]]
             [result-rewriting :refer [rewrite-query-results]]]
            [drafter.util :as util]
            [grafter.rdf.protocols :as proto]
            [schema.core :as s])
  (:import org.openrdf.model.URI))

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

(defrecord RestrictedExecutor [inner restriction]
  backend/SparqlExecutor

  (prepare-query [this query-string]
    (let [pquery (prepare-query inner query-string)]
      (apply-restriction pquery restriction)))

  backend/SparqlUpdateExecutor
  (execute-update [this update-query]
    (execute-restricted-update this update-query restriction))

  backend/ToRepository
  (->sesame-repo [_] (->sesame-repo inner)))

(extend RestrictedExecutor
  proto/ITripleReadable itriple-readable-delegate
  proto/ITripleWriteable itriple-writeable-delegate
  proto/ISPARQLable isparqlable-delegate
  proto/ISPARQLUpdateable isparql-updateable-delegate)

(def create-restricted ->RestrictedExecutor)

(defn- stringify-graph-mapping [live->draft]
  (util/map-all #(.stringValue %) live->draft))

(defn- get-rewritten-query-graph-restriction [db live->draft union-with-live?]
  (mgmt/graph-mapping->graph-restriction db (stringify-graph-mapping live->draft) union-with-live?))

(s/defrecord RewritingSesameSparqlExecutor [inner :- (s/protocol backend/SparqlExecutor)
                                            live->draft :- {URI URI}
                                            union-with-live? :- Boolean]
  backend/SparqlExecutor
  (prepare-query [this sparql-string]
    (let [rewritten-query-string (rewrite-sparql-string live->draft sparql-string)
          graph-restriction (get-rewritten-query-graph-restriction inner live->draft union-with-live?)
          pquery (prepare-query inner rewritten-query-string)
          pquery (apply-restriction pquery graph-restriction)]
      (rewrite-query-results pquery live->draft)))

  backend/ToRepository
  (->sesame-repo [_] (->sesame-repo inner)))

(extend RewritingSesameSparqlExecutor
  proto/ITripleReadable itriple-readable-delegate
  proto/ITripleWriteable itriple-writeable-delegate
  proto/ISPARQLable isparqlable-delegate
  proto/ISPARQLUpdateable isparql-updateable-delegate)

(def draft-graph-set ->RewritingSesameSparqlExecutor)
