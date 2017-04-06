(ns drafter.backend.sesame.remote
  (:require [drafter.backend.repository]
            [clojure.tools.logging :as log]
            [grafter.rdf.repository.registry :as reg])
  (:import drafter.rdf.DrafterSPARQLRepository
           org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONParserFactory
           org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLParserFactory
           org.openrdf.rio.nquads.NQuadsParserFactory
           org.openrdf.query.resultio.sparqljson.SPARQLBooleanJSONParserFactory
           org.openrdf.query.resultio.text.BooleanTextParserFactory
           org.openrdf.query.resultio.sparqlxml.SPARQLBooleanXMLParserFactory
           org.openrdf.rio.ntriples.NTriplesParserFactory))

(defn get-required-configuration-setting [var-key config]
  (if-let [ev (var-key config)]
    ev
    (let [message (str "Missing required configuration key " var-key)]
      (do
        (log/error message)
        (throw (RuntimeException. message))))))

(defn create-sparql-repository [query-endpoint update-endpoint]
  "Creates a new SPARQL repository with the given query and update
  endpoints."
  (let [repo (DrafterSPARQLRepository. query-endpoint update-endpoint)]
    (.initialize repo)
    (log/info "Initialised repo at QUERY=" query-endpoint ", UPDATE=" update-endpoint)
    repo))

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
(def construct-formats-whitelist #{NTriplesParserFactory NQuadsParserFactory})
(def select-formats-whitelist #{SPARQLResultsXMLParserFactory SPARQLResultsJSONParserFactory})
(def ask-formats-whitelist #{SPARQLBooleanJSONParserFactory BooleanTextParserFactory SPARQLBooleanXMLParserFactory})

(defn get-backend [config]
  "Creates a new SPARQL repository with the query and update endpoints
  configured in the given environment map."

  ;; This call here obliterates the sesame defaults for registered
  ;; parsers.  Forcing content negotiation to work only with the
  ;; parsers we explicitly whitelist above.
  (reg/register-parser-factories! {:select select-formats-whitelist
                                   :construct construct-formats-whitelist
                                   :ask ask-formats-whitelist})

  (let [query-endpoint (get-required-configuration-setting :sparql-query-endpoint config)
        update-endpoint (get-required-configuration-setting :sparql-update-endpoint config)]
    (create-sparql-repository query-endpoint update-endpoint)))
