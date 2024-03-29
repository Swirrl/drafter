(ns drafter.backend.rdf4j-repo
  "Thin wrapper over a SparqlRepository as a configurable integrant component."
  (:require [clojure.spec.alpha :as s]
            [drafter.backend :as backend]
            [clojure.tools.logging :as log]
            [grafter-2.rdf4j.repository.registry :as reg]
            [integrant.core :as ig]
            [grafter-2.rdf4j.repository :as repo])
  (:import [org.eclipse.rdf4j.query.resultio.sparqljson SPARQLBooleanJSONParserFactory SPARQLResultsJSONParserFactory]
           [org.eclipse.rdf4j.query.resultio.sparqlxml SPARQLBooleanXMLParserFactory SPARQLResultsXMLParserFactory]
           [org.eclipse.rdf4j.query.resultio.binary BinaryQueryResultParserFactory]
           org.eclipse.rdf4j.query.resultio.text.BooleanTextParserFactory
           org.eclipse.rdf4j.rio.nquads.NQuadsParserFactory
           org.eclipse.rdf4j.rio.ntriples.NTriplesParserFactory
           org.eclipse.rdf4j.rio.turtle.TurtleParserFactory
           org.eclipse.rdf4j.rio.trig.TriGParserFactory
           org.eclipse.rdf4j.rio.binary.BinaryRDFParserFactory))

(defn create-sparql-repository
  "Creates a new SPARQL repository with the given query and update
  endpoints."
  [query-endpoint update-endpoint]
  (let [repo (repo/sparql-repo query-endpoint update-endpoint)]
    (.enableQuadMode repo true)
    (log/info "Initialised repo at QUERY=" query-endpoint ", UPDATE=" update-endpoint)
    repo))

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

(defn get-backend
  "Creates a new SPARQL repository with the query and update endpoints
  configured in the given configuration map."
  [{:keys [sparql-query-endpoint sparql-update-endpoint]}]

  ;; This call here obliterates the sesame defaults for registered
  ;; parsers.  Forcing content negotiation to work only with the
  ;; parsers we explicitly whitelist above.
  (reg/register-parser-factories! {:select select-formats-whitelist
                                   :construct construct-formats-whitelist
                                   :ask ask-formats-whitelist})

  (create-sparql-repository (str sparql-query-endpoint) (str sparql-update-endpoint)))


(defmethod ig/pre-init-spec :drafter.backend/rdf4j-repo [_]
  (s/keys :req-un [::backend/sparql-query-endpoint ::backend/sparql-update-endpoint]))

(defmethod ig/init-key :drafter.backend/rdf4j-repo [k opts]
  (log/info "Initialising Backend")
  (get-backend opts))

(defmethod ig/halt-key! :drafter.backend/rdf4j-repo [_k repo]
  (log/info "Halting Backend")
  (repo/shutdown repo))
