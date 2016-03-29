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

(defn- delete-quads-from-draftset [backend quad-batches draftset-ref live->draft {:keys [op job-started-at] :as state} job]
  (case op
    :delete
    (if-let [batch (first quad-batches)]
      (let [live-graph (context (first batch))]
        (if (mgmt/is-graph-managed? backend live-graph)
          (if-let [draft-graph-uri (get live->draft live-graph)]
            (do
              (mgmt/set-modifed-at-on-draft-graph! backend draft-graph-uri job-started-at)
              (with-open [conn (repo/->connection (->sesame-repo backend))]
                (let [rewritten-statements (map #(rewrite-statement live->draft %) batch)
                      sesame-statements (map IStatement->sesame-statement rewritten-statements)
                      graph-array (into-array Resource (map util/string->sesame-uri (vals live->draft)))]
                  (.remove conn sesame-statements graph-array)))
              (let [next-job (create-child-job
                              job
                              (partial delete-quads-from-draftset backend (rest quad-batches) draftset-ref live->draft state))]
                (scheduler/queue-job! next-job)))
            ;;NOTE: Do this immediately as we haven't done any real work yet
            (recur backend quad-batches draftset-ref live->draft (merge state {:op :copy-graph :live-graph live-graph}) job))
          ;;live graph does not exist so do not create a draft graph
          ;;NOTE: This is the same behaviour as deleting a live graph
          ;;which does not exist in live
          (recur backend (rest quad-batches) draftset-ref live->draft state job)))
      (let [draftset-info (dsmgmt/get-draftset-info backend draftset-ref)]
        (jobs/job-succeeded! job {:draftset draftset-info})))

    :copy-graph
    (let [{:keys [live-graph]} state
          draft-graph-uri (mgmt/create-draft-graph! backend live-graph {} (str (ds/->draftset-uri draftset-ref)))
          copy-batches (jobs/get-graph-clone-batches backend live-graph)
          copy-state {:op :copy-graph-batches
                      :graph live-graph
                      :draft-graph-uri draft-graph-uri
                      :batches copy-batches}]
      ;;NOTE: Do this immediately as no work has been done yet
      (recur backend quad-batches draftset-ref (assoc live->draft live-graph draft-graph-uri) (merge state copy-state) job))

    :copy-graph-batches
    (let [{:keys [graph batches draft-graph-uri]} state]
      (if-let [[offset limit] (first batches)]
        (do
          (jobs/copy-graph-batch! backend graph draft-graph-uri offset limit)
          (let [next-state (update-in state [:batches] rest)
                next-job (create-child-job
                          job
                          (partial delete-quads-from-draftset backend quad-batches draftset-ref live->draft (merge state next-state)))]
            (scheduler/queue-job! next-job)))
        ;;graph copy completed so continue deleting quads
        ;;NOTE: do this immediately since we haven't done any work on this iteration
        (recur backend quad-batches draftset-ref live->draft (merge state {:op :delete}) job)))))

(defn- batch-and-delete-quads-from-draftset [backend quads draftset-ref live->draft job]
  (let [quad-batches (util/batch-partition-by quads context jobs/batched-write-size)
        now (java.util.Date.)]
    (delete-quads-from-draftset backend quad-batches draftset-ref live->draft {:op :delete :job-started-at now} job)))

(defn- rdf-handler->spog-tuple-handler [rdf-handler]
  (reify TupleQueryResultHandler
    (handleSolution [this bindings]
      (let [subj (.getValue bindings "s")
            pred (.getValue bindings "p")
            obj (.getValue bindings "o")
            graph (.getValue bindings "g")
            stmt (ContextStatementImpl. subj pred obj graph)]
        (.handleStatement rdf-handler stmt)))

    (handleBoolean [this b])
    (handleLinks [this links])
    (startQueryResult [this binding-names]
      (.startRDF rdf-handler))
    (endQueryResult [this]
      (.endRDF rdf-handler))))

(defn- spog-tuple-query->graph-query [tuple-query]
  (reify GraphQuery
    (evaluate [this rdf-handler]
      (.evaluate tuple-query (rdf-handler->spog-tuple-handler rdf-handler)))))

(defn all-quads-query [backend]
  (let [tuple-query (backend/prepare-query backend "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }")]
    (spog-tuple-query->graph-query tuple-query)))

(defn execute-update-with [exec-prepared-update-fn backend update-query restrictions]
  (with-open [conn (->repo-connection backend)]
      (let [dataset (restricted-dataset restrictions)
            pquery (repo/prepare-update conn update-query dataset)]
        (exec-prepared-update-fn conn pquery))))

(defn- execute-prepared-update-in-transaction [conn prepared-query]
  (repo/with-transaction conn
    (repo/evaluate prepared-query)))

(defn execute-update [backend update-query restrictions]
  (execute-update-with execute-prepared-update-in-transaction backend update-query restrictions))

(defn- stringify-graph-mapping [live->draft]
  (util/map-all #(.stringValue %) live->draft))

(defn- get-rewritten-query-graph-restriction [db live->draft union-with-live?]
  (mgmt/graph-mapping->graph-restriction db (stringify-graph-mapping live->draft) union-with-live?))

(s/defrecord RewritingSesameSparqlExecutor [db :- Repository
                                            live->draft :- {URI URI}
                                            union-with-live? :- Boolean]
  backend/SparqlExecutor
  (all-quads-query [this]
    (all-quads-query this))

  (prepare-query [this sparql-string]
    (let [rewritten-query-string (rewrite-sparql-string live->draft sparql-string)
          graph-restriction (get-rewritten-query-graph-restriction db live->draft union-with-live?)
          prepared-query (prepare-restricted-query this rewritten-query-string graph-restriction)]
      (rewrite-query-results prepared-query live->draft)))

  (get-query-type [_ pquery]
    (get-query-type db pquery))

  (create-query-executor [_ writer-fn pquery]
    (create-query-executor db writer-fn pquery))

  backend/StatementDeletion
  (delete-quads-from-draftset-job [this serialised rdf-format draftset-ref]
    (jobs/make-job
     :batch-write [job]
     (let [quads (read-statements serialised rdf-format)]
       (batch-and-delete-quads-from-draftset this quads draftset-ref (stringify-graph-mapping live->draft) job))))

  (delete-triples-from-draftset-job [this serialised rdf-format draftset-ref graph]
    (jobs/make-job
     :batch-write [job]
     (let [triples (read-statements serialised rdf-format)
           quads (map #(util/make-quad-statement % graph) triples)]
       (batch-and-delete-quads-from-draftset this quads draftset-ref (stringify-graph-mapping live->draft) job))))

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
  (all-quads-query [this]
    (all-quads-query this))

  (prepare-query [this query-string]
    (let [pquery (prepare-query this query-string)]
      (apply-restriction pquery restriction)))

  (get-query-type [this pquery]
    (get-query-type this pquery))

  (create-query-executor [this result-format pquery]
    (create-query-executor this result-format pquery))

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
