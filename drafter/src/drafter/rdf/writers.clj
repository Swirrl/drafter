(ns drafter.rdf.writers
  (:require [drafter.rdf.formats :refer [csv-rdf-format tsv-rdf-format]])
  (:import [java.io OutputStream Writer]
           [org.eclipse.rdf4j.query.impl MapBindingSet]
           [org.eclipse.rdf4j.query.resultio QueryResultWriter]
           [org.eclipse.rdf4j.query.resultio.text.csv SPARQLResultsCSVWriter]
           [org.eclipse.rdf4j.query.resultio.text.tsv SPARQLResultsTSVWriter]
           [org.eclipse.rdf4j.rio RDFWriter RDFWriterFactory RDFWriterRegistry]))

(defn- query-result-writer->rdf-writer [rdf-format ^QueryResultWriter result-writer]
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

(defn register-custom-rdf-writers! []
  (register-rdf-writer-factory (create-rdf-writer-factory csv-rdf-format create-csv-result-writer))
  (register-rdf-writer-factory (create-rdf-writer-factory tsv-rdf-format create-tsv-result-writer)))

