(ns drafter.rdf.sesame
  (:require [drafter.backend.draftset.arq :refer [sparql-string->arq-query]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [grafter-2.rdf4j.io :refer [statements] :as gio]
            [grafter.url :as url]
            [grafter-2.rdf4j.io :refer [statements]]
            [grafter-2.rdf.protocols :as pr]
            [drafter.util :as util]
            [grafter-2.rdf4j.io :as rio])
  (:import [org.eclipse.rdf4j.query BindingSet BooleanQuery GraphQuery TupleQuery TupleQueryResultHandler TupleQueryResult Update Binding]
           [org.eclipse.rdf4j.query.resultio QueryResultIO QueryResultWriter]
           [org.eclipse.rdf4j.rio RDFHandler Rio RDFFormat]
           [org.eclipse.rdf4j.common.iteration Iteration]
           [org.eclipse.rdf4j.repository RepositoryConnection]
           [org.eclipse.rdf4j.model Resource IRI Value]
           [org.eclipse.rdf4j.query.impl MapBindingSet]
           (java.net URI)))

(defn is-quads-format? [^RDFFormat rdf-format]
  (.supportsContexts rdf-format))

(defn is-triples-format? [rdf-format]
  (not (is-quads-format? rdf-format)))

(extend-protocol url/IURIable
  org.eclipse.rdf4j.model.URI
  (->java-uri [rdf4j-uri]
    (java.net.URI. (str rdf4j-uri))))

(defn iteration-seq
  "Returns lazy sequence over an RDF4j Iteration"
  [^Iteration iter]
  (lazy-seq
    (when (.hasNext iter)
      (let [v (.next iter)]
        (cons v (iteration-seq iter))))))

(defn get-statements
  "Returns a sequence of all the RDF4j statements within "
  [^RepositoryConnection conn infer graph-uris]
  (let [resources (into-array Resource (map gio/->rdf4j-uri graph-uris))
        ^Resource subj nil
        ^IRI pred nil
        ^Value obj nil
        iter (.getStatements conn subj pred obj infer resources)]
    (map gio/backend-quad->grafter-quad (iteration-seq iter))))

(defn binding-set->map
  "Converts an RDF4j BindingSet into a map"
  [^BindingSet bindings]
  (->> (.iterator bindings)
       (iterator-seq)
       (map (fn [^Binding b] [(.getName b) (.getValue b)]))
       (into {})))

(defn map->binding-set
  "Creates an RDF4j BindingSet given a map of binding names to grafter values"
  [m]
  (let [bs (MapBindingSet.)]
    (doseq [[k v] m]
      (.addBinding bs (name k) (rio/->backend-type v)))
    bs))

(defn read-statements
  "Creates a lazy stream of statements from an input stream containing
  RDF data serialised in the given format."
  ([input rdf-format] (read-statements input rdf-format jobs/batched-write-size))
  ([input rdf-format batch-size]
   (statements input
               :format rdf-format
               :buffer-size batch-size)))

(defrecord FormatStatementSource [inner-source format]
  pr/ITripleReadable
  (to-statements [_this _options]
    (read-statements inner-source format)))

;; NOTE: The purpose of this is to stomp `graph` over all statements
;; in the sequence if `graph` is set.
(defrecord GraphTripleStatementSource [triple-source graph]
  pr/ITripleReadable
  (to-statements [_this options]
    (let [statements (pr/to-statements triple-source options)]
      (if graph
        (map #(util/make-quad graph %) statements)
        statements))))

(defrecord CollectionStatementSource [statements]
  pr/ITripleReadable
  (to-statements [_this _options]
    statements))

(defn get-query-type
  "Returns a keyword indicating the type query represented by the
  given prepared query. Returns nil if the query could not be
  classified."
  [pquery]
  (condp instance? pquery
      TupleQuery :select
      BooleanQuery :ask
      GraphQuery :construct
      Update :update
      nil))

(defn create-tuple-query-writer [os result-format]
  (QueryResultIO/createWriter result-format os))

(defn create-construct-query-writer [os result-format]
  (Rio/createWriter result-format os))

(defn signalling-tuple-query-handler
  [signal ^BindingSet binding-set ^QueryResultWriter writer]
  (reify
    TupleQueryResult
    (getBindingNames [this]
      (let [is (iterator-seq (.iterator binding-set))]
        (if (seq is)
          []
          is)))

    (close [this]
      ;; no-op can't call .close on writers
      )

    TupleQueryResultHandler
    (startQueryResult [this binding-names]
      (deliver signal :ok)
      (.startQueryResult writer binding-names))
    (endQueryResult [this]
      (.endQueryResult writer))
    (handleSolution [this binding-set]
      (.handleSolution writer binding-set))
    (handleBoolean [this b]
      (.handleBoolean writer b))
    (handleLinks [this link-urls]
      (.handleLinks writer link-urls))))

(defn signalling-rdf-handler [signal ^RDFHandler handler]
  (reify RDFHandler
    (startRDF [this]
      (deliver signal :ok)
      (.startRDF handler))
    (endRDF [this]
      (.endRDF handler))
    (handleNamespace [this prefix uri]
      (.handleNamespace handler prefix uri))
    (handleStatement [this s]
      (.handleStatement handler s))
    (handleComment [this comment]
      (.handleComment handler comment))))

(defn create-signalling-query-handler
  [pquery output-stream result-format signal]
  (case (get-query-type pquery)
    :select (signalling-tuple-query-handler
              signal
              (.getBindings pquery)
              (create-tuple-query-writer output-stream result-format))
    :construct (signalling-rdf-handler
                 signal
                 (create-construct-query-writer output-stream result-format))
    (throw (IllegalArgumentException. "Query must be either a SELECT or CONSTRUCT query."))))
