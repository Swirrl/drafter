(ns drafter.rdf.sesame
  (:import [java.util ArrayList]
           [org.openrdf.rio Rio]
           [org.openrdf.rio.helpers StatementCollector]))

(defn is-quads-format? [rdf-format]
  (.supportsContexts rdf-format))

(defn is-triples-format? [rdf-format]
  (not (is-quads-format? rdf-format)))

(defn parse-stream-statements [in-stream format]
  (let [parser (Rio/createParser format)
        model (ArrayList.)
        base-uri ""]
    (.setRDFHandler parser (StatementCollector. model))
    (.parse parser in-stream base-uri)
    (seq model)))
