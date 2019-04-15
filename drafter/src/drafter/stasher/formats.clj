(ns drafter.stasher.formats
  (:import [org.eclipse.rdf4j.query.resultio.sparqljson SPARQLBooleanJSONParserFactory SPARQLResultsJSONParserFactory]
           [org.eclipse.rdf4j.query.resultio.sparqlxml SPARQLBooleanXMLParserFactory SPARQLResultsXMLParserFactory]
           [org.eclipse.rdf4j.query.resultio.binary BinaryQueryResultParserFactory]
           org.eclipse.rdf4j.query.resultio.text.BooleanTextParserFactory
           org.eclipse.rdf4j.rio.nquads.NQuadsParserFactory
           org.eclipse.rdf4j.rio.ntriples.NTriplesParserFactory
           org.eclipse.rdf4j.rio.turtle.TurtleParserFactory
           org.eclipse.rdf4j.rio.trig.TriGParserFactory
           org.eclipse.rdf4j.rio.binary.BinaryRDFParserFactory
           (org.eclipse.rdf4j.rio RDFFormat RDFHandler RDFWriter RDFParserRegistry)
           (org.eclipse.rdf4j.query.resultio TupleQueryResultFormat BooleanQueryResultFormat QueryResultIO
                                             TupleQueryResultWriter BooleanQueryResultParserRegistry
                                             TupleQueryResultParserRegistry))
  (:require [clojure.spec.alpha :as s]))

(defn- build-format-keyword->format-map [formats]
  "Builds a hashmap from format keywords to RDFFormat's.

  e.g. nt => RDFFormat/NTRIPLES etc..."
  (reduce (fn [acc fmt]
            (merge acc
                   (zipmap (map keyword (.getFileExtensions fmt))
                           (repeat fmt))))
          {}
          formats))


(def rdf-formats [RDFFormat/BINARY
                  RDFFormat/NTRIPLES
                  RDFFormat/RDFXML
                  RDFFormat/RDFJSON
                  RDFFormat/TURTLE])

(def tuple-formats [TupleQueryResultFormat/BINARY
                    TupleQueryResultFormat/SPARQL
                    TupleQueryResultFormat/JSON])

(def boolean-formats [BooleanQueryResultFormat/TEXT
                      BooleanQueryResultFormat/JSON
                      BooleanQueryResultFormat/SPARQL])

(s/def ::query-type-keyword #{:graph :tuple :boolean})

(def supported-cache-formats
  {:graph (build-format-keyword->format-map rdf-formats)
   :tuple (build-format-keyword->format-map tuple-formats)
   :boolean (build-format-keyword->format-map boolean-formats)})

(s/def ::cache-format-keyword (set (mapcat keys (vals supported-cache-formats))))

;; TODO:
;;
;; We should turn these whitelist sets into proper configuration.
;;
;; Set some whitelists that ensure we're much more strict around what
;; formats we negotiate with stardog.  If you want to run drafter
;; against another (non stardog) store we should configure these to be
;; different.
;;
;; For construct we avoid Turtle because of Stardog bug #3087
;; (https://complexible.zendesk.com/hc/en-us/requests/524)
;;
;; Also we avoid RDF+XML because RDF+XML can't even represent some RDF graphs:
;; https://www.w3.org/TR/REC-rdf-syntax/#section-Serialising which
;; causes us some issues when URI's for predicates contain parentheses.
;;
;; Ideally we'd just run with sesame's defaults, but providing a
;; smaller list should mean less bugs in production as we can choose
;; the most reliable formats and avoid those with known issues.
;;
(def construct-formats-whitelist #{TurtleParserFactory NTriplesParserFactory NQuadsParserFactory TriGParserFactory BinaryRDFParserFactory})
(def select-formats-whitelist #{SPARQLResultsXMLParserFactory SPARQLResultsJSONParserFactory BinaryQueryResultParserFactory})
(def ask-formats-whitelist #{SPARQLBooleanJSONParserFactory BooleanTextParserFactory SPARQLBooleanXMLParserFactory})
