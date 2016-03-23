(ns drafter.backend.sesame.common
  (:require [grafter.rdf.repository :as repo]
            [grafter.rdf :refer [add statements]]
            [clojure.tools.logging :as log]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.draftset-management :as dsmgmt]
            [grafter.rdf.protocols :as proto]
            [drafter.backend.sesame.common.protocols :refer [->sesame-repo]]
            [drafter.backend.sesame.common.sparql-execution :as sparql]))

;;SPARQL execution
(def default-sparql-query-impl
  {:all-quads-query sparql/all-quads-query
   :prepare-query sparql/prepare-query
   :get-query-type sparql/get-query-type
   :negotiate-result-writer sparql/negotiate-result-writer
   :create-query-executor sparql/create-query-executor})

(def default-query-rewritable-impl
  {:create-rewriter sparql/->RewritingSesameSparqlExecutor})

(def default-sparql-update-impl
  {:execute-update sparql/execute-update})

;;stoppable
(def default-stoppable-impl
  {:stop (fn [x] (repo/shutdown (->sesame-repo x)))})

;;Grafter SPARQL protocols

;;ITripleReadable
(def default-triple-readable-impl
  {:to-statements (fn [this options]
                    (proto/to-statements (->sesame-repo this) options))})

;;ISPARQLable
(def default-sparqlable-impl
  {:query-dataset (fn [this sparql-string model]
                    (proto/query-dataset (->sesame-repo this) sparql-string model))})

(def default-isparql-updatable-impl
  {:update! (fn [this sparql-string]
              (proto/update! (->sesame-repo this) sparql-string))})

;;ITripleWritable
(defn- add-statement-impl
  ([this statement] (proto/add-statement (->sesame-repo this) statement))
  ([this graph statement] (proto/add-statement (->sesame-repo this) graph statement)))

(defn- add-impl
  ([this triples] (proto/add (->sesame-repo this) triples))
  ([this graph triples] (proto/add (->sesame-repo this) graph triples))
  ([this graph format triple-stream] (proto/add (->sesame-repo this) format triple-stream))
  ([this graph base-uri format triple-stream] (proto/add (->sesame-repo this) format triple-stream)))

(def default-triple-writeable-impl
  {:add-statement add-statement-impl
   :add add-impl})
