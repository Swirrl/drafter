(ns drafter.backend.sesame.common.sparql-execution
  (:require [clojure.tools.logging :as log]
            [schema.core :as s]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :refer [map->Quad] :as gproto]
            [grafter.rdf :refer [context]]
            [grafter.rdf.io :refer [IStatement->sesame-statement]]
            [clojure.set :as set]
            [drafter.backend.protocols :refer [->sesame-repo] :as backend]
            [drafter.write-scheduler :as scheduler]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.draftset :as ds]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.sesame :refer [read-statements get-query-type]]
            [swirrl-server.async.jobs :refer [create-job create-child-job]]
            [drafter.rdf.rewriting.query-rewriting :refer [rewrite-sparql-string]]
            [drafter.rdf.rewriting.result-rewriting :refer [rewrite-query-results rewrite-statement]]
            [drafter.rdf.rewriting.arq :refer [->sparql-string sparql-string->arq-query]]
            [drafter.util :refer [map-values] :as util])
  (:import [org.openrdf.query TupleQuery TupleQueryResult
            TupleQueryResultHandler BooleanQueryResultHandler
            BindingSet QueryLanguage BooleanQuery GraphQuery Update]
           [org.openrdf.repository Repository]
           [org.openrdf.repository.sparql SPARQLRepository]
           [org.openrdf.rio Rio RDFWriter RDFHandler]
           [org.openrdf.query.resultio QueryResultWriter QueryResultIO]
           [org.openrdf.query Dataset]
           [org.openrdf.query.impl MapBindingSet]
           [org.openrdf.model Resource URI]
           [org.openrdf.model.impl ContextStatementImpl URIImpl]))

(defn- get-restrictions [graph-restrictions]
  (cond
   (coll? graph-restrictions) graph-restrictions
   (fn? graph-restrictions) (graph-restrictions)
   :else nil))

(defn restricted-dataset
  "Returns a restricted dataset or nil when given either a 0-arg
  function or a collection of graph uris."
  [graph-restrictions]
  {:pre [(or (nil? graph-restrictions)
             (coll? graph-restrictions)
             (fn? graph-restrictions))]
   :post [(or (instance? Dataset %)
              (nil? %))]}
  (when-let [graph-restrictions (get-restrictions graph-restrictions)]
    (repo/make-restricted-dataset :default-graph graph-restrictions
                                    :named-graphs graph-restrictions)))

(defn apply-restriction [pquery restriction]
  (let [dataset (restricted-dataset restriction)]
    (.setDataset pquery dataset)
    pquery))

(defmulti exec-sesame-prepared-update (fn [repo prepare-fn] (class repo)))
(defmethod exec-sesame-prepared-update SPARQLRepository [repo prepare-fn]
  ;;default sesame implementation executes UPDATE queries in a
  ;;transaction which the remote SPARQL client does not like
  (with-open [conn (repo/->connection repo)]
    (let [pquery (prepare-fn conn)]
      (repo/evaluate pquery))))

(defmethod exec-sesame-prepared-update Repository [repo prepare-fn]
  (with-open [conn (repo/->connection repo)]
    (repo/with-transaction conn
      (let [pquery (prepare-fn conn)]
        (repo/evaluate pquery)))))

(defn execute-restricted-update [backend update-query restrictions]
  (exec-sesame-prepared-update
   (->sesame-repo backend)
   (fn [conn]
     (repo/prepare-update conn update-query (restricted-dataset restrictions)))))

(defn execute-update [backend update-query]
  (execute-restricted-update backend update-query nil))
