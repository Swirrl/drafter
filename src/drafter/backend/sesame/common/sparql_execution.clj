(ns drafter.backend.sesame.common.sparql-execution
  (:require [clojure.tools.logging :as log]
            [schema.core :as s]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :refer [map->Quad] :as gproto]
            [grafter.rdf :refer [context]]
            [grafter.rdf.io :refer [IStatement->sesame-statement]]
            [clojure.set :as set]
            [drafter.backend.protocols :as backend]
            [drafter.backend.sesame.common.protocols :refer :all]
            [drafter.write-scheduler :as scheduler]
            [drafter.rdf.draft-management :as mgmt]
            [drafter.draftset :as ds]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.rdf.sesame :refer [read-statements]]
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

(defn notifying-query-result-handler [notify-fn inner-handler]
  (reify
    TupleQueryResultHandler
    (handleBoolean [this b]
      (notify-fn)
      (.handleBoolean inner-handler b))
    (handleLinks [this links] (.handleLinks inner-handler links))
    (startQueryResult [this binding-names] (.startQueryResult inner-handler binding-names))
    (endQueryResult [this] (.endQueryResult inner-handler))
    (handleSolution [this binding-set]
      (notify-fn)
      (.handleSolution inner-handler binding-set))))

(defn notifying-rdf-handler [notify-fn inner-handler]
  (reify
    RDFHandler
    (startRDF [this]
      (.startRDF inner-handler))
    (endRDF [this]
      (.endRDF inner-handler))
    (handleNamespace [this prefix uri]
      (.handleNamespace inner-handler prefix uri))
    (handleStatement [this statement]
      (notify-fn)
      (.handleStatement inner-handler statement))
    (handleComment [this comment]
      (.handleComment inner-handler comment))))

(defn- exec-ask-query [writer pquery result-notify-fn]
  (let [notifying-handler (notifying-query-result-handler result-notify-fn writer)
           result (.evaluate pquery)]
       (doto notifying-handler
         (.handleBoolean result))))

(defn- exec-tuple-query [writer pquery result-notify-fn]
  (log/debug "pquery (default) is " pquery " writer is " writer)
  (.evaluate pquery (notifying-query-result-handler result-notify-fn writer)))

(defn- exec-graph-query [writer pquery result-notify-fn]
  (log/debug "pquery is " pquery " writer is " writer)
  (let [notifying-handler (notifying-rdf-handler result-notify-fn writer)]
    (.evaluate pquery notifying-handler)))

(defn get-query-type [backend pquery]
  (condp instance? pquery
      TupleQuery :select
      BooleanQuery :ask
      GraphQuery :construct
      Update :update
      nil))

(defn create-query-executor [backend result-format pquery]
  (case (get-query-type backend pquery)
    :select (fn [os notifier-fn]
              (let [w (QueryResultIO/createWriter result-format os)]
                (exec-tuple-query w pquery notifier-fn)))

    :ask (fn [os notifier-fn]
           (let [w (QueryResultIO/createWriter result-format os)]
             (exec-ask-query w pquery notifier-fn)))

    :construct (fn [os notifier-fn]
                 (let [w (Rio/createWriter result-format os)]
                   (exec-graph-query w pquery notifier-fn)))
    (throw (IllegalArgumentException. (str "Invalid query type")))))

(defn validate-query [query-str]
  "Validates a query by parsing it using ARQ. If the query is invalid
  a QueryParseException is thrown."
  (sparql-string->arq-query query-str)
  query-str)

(defn prepare-query [backend sparql-string]
    (let [repo (->sesame-repo backend)
          validated-query-string (validate-query sparql-string)]
      (repo/prepare-query repo validated-query-string)))

(defn- apply-restriction [pquery restriction]
  (let [dataset (restricted-dataset restriction)]
    (.setDataset pquery dataset)
    pquery))

(defn- prepare-restricted-query [backend sparql-string graph-restriction]
  (let [pquery (prepare-query backend sparql-string)]
    (apply-restriction pquery graph-restriction)))

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

(defn- execute-restricted-update [backend update-query restrictions]
  (exec-sesame-prepared-update
   (->sesame-repo backend)
   (fn [conn]
     (repo/prepare-update conn update-query (restricted-dataset restrictions)))))

(defn execute-update [backend update-query]
  (execute-restricted-update backend update-query #{}))

(defn- stringify-graph-mapping [live->draft]
  (util/map-all #(.stringValue %) live->draft))

(defn- get-rewritten-query-graph-restriction [db live->draft union-with-live?]
  (mgmt/graph-mapping->graph-restriction db (stringify-graph-mapping live->draft) union-with-live?))

(s/defrecord RewritingSesameSparqlExecutor [db :- Repository
                                            live->draft :- {URI URI}
                                            union-with-live? :- Boolean]
  backend/SparqlExecutor
  (prepare-query [this sparql-string]
    (let [rewritten-query-string (rewrite-sparql-string live->draft sparql-string)
          graph-restriction (get-rewritten-query-graph-restriction db live->draft union-with-live?)
          prepared-query (prepare-restricted-query this rewritten-query-string graph-restriction)]
      (rewrite-query-results prepared-query live->draft)))

  (get-query-type [_ pquery]
    (get-query-type db pquery))

  (create-query-executor [_ writer-fn pquery]
    (create-query-executor db writer-fn pquery))

  gproto/ITripleReadable
  (to-statements [_ options] (gproto/to-statements db options))

  gproto/ISPARQLable
  (query-dataset [_ sparql-string model] (gproto/query-dataset db sparql-string model))

  gproto/ISPARQLUpdateable
  (update! [this sparql-string] (gproto/update! db sparql-string))
  
  ToRepository
  (->sesame-repo [_] db))

(extend-type RewritingSesameSparqlExecutor
  gproto/ITripleWriteable
  (add
    ([this triples] (gproto/add (->sesame-repo this) triples))
    ([this graph triples] (gproto/add (->sesame-repo this) graph triples))
    ([this graph format triple-stream] (gproto/add (->sesame-repo this) graph format triple-stream))
    ([this graph base-uri format triple-stream] (gproto/add (->sesame-repo this) graph base-uri format triple-stream)))

  (add-statement
    ([this statement] (gproto/add-statement (->sesame-repo this) statement))
    ([this graph statement] (gproto/add-statement (->sesame-repo this) graph statement))))

(defrecord RestrictedExecutor [db restriction]
  backend/SparqlExecutor

  (prepare-query [this query-string]
    (let [pquery (prepare-query this query-string)]
      (apply-restriction pquery restriction)))

  (get-query-type [this pquery]
    (get-query-type this pquery))

  (create-query-executor [this result-format pquery]
    (create-query-executor this result-format pquery))

  backend/SparqlUpdateExecutor
  (execute-update [this update-query]
    (execute-restricted-update this update-query restriction))

  gproto/ITripleReadable
  (to-statements [_ options] (gproto/to-statements db options))

  gproto/ISPARQLable
  (query-dataset [_ sparql-string model] (gproto/query-dataset db sparql-string model))

  gproto/ISPARQLUpdateable
  (update! [this sparql-string] (gproto/update! db sparql-string))

  ToRepository
  (->sesame-repo [_] db))

(extend-type RestrictedExecutor
  gproto/ITripleWriteable
  (add
    ([this triples] (gproto/add (->sesame-repo this) triples))
    ([this graph triples] (gproto/add (->sesame-repo this) graph triples))
    ([this graph format triple-stream] (gproto/add (->sesame-repo this) graph format triple-stream))
    ([this graph base-uri format triple-stream] (gproto/add (->sesame-repo this) graph base-uri format triple-stream)))

  (add-statement
    ([this statement] (gproto/add-statement (->sesame-repo this) statement))
    ([this graph statement] (gproto/add-statement (->sesame-repo this) graph statement))))
