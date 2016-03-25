(ns drafter.rdf.content-negotiation
  (:require [drafter.rdf.rewriting.result-rewriting :refer [result-handler-wrapper]]
            [clojure.java.io :as io]
            [grafter.rdf.formats :as formats]
            [ring.middleware.accept :refer [wrap-accept]])
  (:import [java.nio.charset Charset]
           [java.io Writer OutputStream]
           [org.openrdf.query.impl MapBindingSet]
           [org.openrdf.rio RDFFormat RDFWriter RDFWriterFactory RDFWriterRegistry]
           [org.openrdf.query.resultio BooleanQueryResultFormat TupleQueryResultFormat]
           [org.openrdf.query.resultio.text.csv SPARQLResultsCSVWriter]
           [org.openrdf.query.resultio.text.tsv SPARQLResultsTSVWriter]))

(def ^:private ascii (Charset/forName "US-ASCII"))
(def csv-rdf-format (RDFFormat. "CSV" "text/csv" ascii "csv" false true))
(def tsv-rdf-format (RDFFormat. "TSV" "text/tab-separated-values" nil "tsv" false true))

(defn- query-result-writer->rdf-writer [rdf-format result-writer]
  (reify RDFWriter
    (getRDFFormat [this] rdf-format)
    (getWriterConfig [this] (.getWriterConfig result-writer))
    (setWriterConfig [this config] (.setWriterConfig result-writer config))
    (getSupportedSettings [this] (.getSupportedSettings result-writer))
    (startRDF [this]
      (.startQueryResult result-writer '("s" "p" "o")))
    (endRDF [this]
      (.endQueryResult result-writer))
    (handleNamespace [this prefix uri]
      ;; No op
      )
    (handleStatement [this statement]
      (let [s (.getSubject statement)
            p (.getPredicate statement)
            o (.getObject statement)
            bs (doto (MapBindingSet.)
                 (.addBinding "s" s)
                 (.addBinding "p" p)
                 (.addBinding "o" o))]
        (.handleSolution result-writer bs)))
    (handleComment [this comment]
      ;; No op
      )))

(defn- create-rdf-writer-factory [rdf-format writer-fn]
  (reify RDFWriterFactory
    (getRDFFormat [this] rdf-format)
    (^RDFWriter getWriter [this ^OutputStream os]
      (query-result-writer->rdf-writer rdf-format (writer-fn os)))
    (^RDFWriter getWriter [this ^Writer w]
      (throw (RuntimeException. "Not supported - use OutputStream overload")))))

(defn- get-rdf-writer-registry []
  (RDFWriterRegistry/getInstance))

(defn- register-rdf-writer-factory [writer-factory]
  (.add (get-rdf-writer-registry) writer-factory))

(defn- create-csv-result-writer [output-stream]
  (SPARQLResultsCSVWriter. output-stream))

(defn- create-tsv-result-writer [output-stream]
  (SPARQLResultsTSVWriter. output-stream))

(register-rdf-writer-factory (create-rdf-writer-factory csv-rdf-format create-csv-result-writer))
(register-rdf-writer-factory (create-rdf-writer-factory tsv-rdf-format create-tsv-result-writer))

(defn- format-preferences->mime-spec [format-prefs]
  (into {} (mapcat (fn [[f q]] (map (fn [m] [m [f q]]) (.getMIMETypes f))) format-prefs)))

(def graph-query-mime-spec
  (merge (format-preferences->mime-spec
          {RDFFormat/NTRIPLES 1.0
           RDFFormat/NQUADS 0.9
           RDFFormat/N3 0.8
           RDFFormat/TRIG 0.8
           RDFFormat/TRIX 0.8
           RDFFormat/TURTLE 0.9
           RDFFormat/RDFXML 0.9
           RDFFormat/RDFJSON 0.9
           csv-rdf-format 0.8
           tsv-rdf-format 0.7})
         {"text/html" [RDFFormat/TURTLE 0.8]}))

(def boolean-query-mime-spec
  (merge (format-preferences->mime-spec
          {BooleanQueryResultFormat/SPARQL 1.0
           BooleanQueryResultFormat/JSON 1.0
           BooleanQueryResultFormat/TEXT 0.9})
         {"text/html" [BooleanQueryResultFormat/TEXT 0.8]
          "text/plain" [BooleanQueryResultFormat/TEXT 0.9]}))

(def tuple-query-mime-spec
  (merge (format-preferences->mime-spec
          {TupleQueryResultFormat/CSV 1.0
           TupleQueryResultFormat/JSON 0.9
           TupleQueryResultFormat/SPARQL 0.9
           TupleQueryResultFormat/BINARY 0.7
           TupleQueryResultFormat/TSV 0.7})
         {"text/plain" [TupleQueryResultFormat/CSV 1.0]
          "text/html" [TupleQueryResultFormat/CSV 1.0]}))

(defn- mime-pref [mime q] [mime :as mime :qs q])

(defn- mime-spec->mime-preferences [spec]
  (vec (mapcat (fn [[m [_ q]]] (mime-pref m q)) spec)))

(defn- get-mime-spec [query-type]
  (case query-type
    :select tuple-query-mime-spec
    :ask boolean-query-mime-spec
    :construct graph-query-mime-spec
    nil))

(defn negotiate
  "Returns the preferred sesame query result format to use for the
  client's accepted mime types. The format returned depends on the
  type of the query (:ask, :select or :construct) to be executed. If
  no acceptable format is available, nil is returned."
  [query-type accept]
  (if-let [spec (get-mime-spec query-type)]
    (let [prefs (mime-spec->mime-preferences spec)
          request {:headers {"accept" accept}}
          handler (wrap-accept identity {:mime prefs})]
      (if-let [mime (get-in (handler request) [:accept :mime])]
        (let [[format _] (get spec mime)]
          [format mime])))))
