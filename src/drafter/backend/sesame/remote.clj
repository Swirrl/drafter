(ns drafter.backend.sesame.remote
  (:require [clojure.string :as str]
            [drafter.backend.protocols :as drpr]
            [clojure.tools.logging :as log]
            [grafter.rdf.repository.registry :as reg]
            [integrant.core :as ig])
  (:import drafter.rdf.DrafterSPARQLRepository
           [org.eclipse.rdf4j.query.resultio.sparqljson SPARQLBooleanJSONParserFactory SPARQLResultsJSONParserFactory]
           [org.eclipse.rdf4j.query.resultio.sparqlxml SPARQLBooleanXMLParserFactory SPARQLResultsXMLParserFactory]
           org.eclipse.rdf4j.query.resultio.text.BooleanTextParserFactory
           org.eclipse.rdf4j.rio.nquads.NQuadsParserFactory
           org.eclipse.rdf4j.rio.ntriples.NTriplesParserFactory))

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
  configured in the given configuration map."

  ;; This call here obliterates the sesame defaults for registered
  ;; parsers.  Forcing content negotiation to work only with the
  ;; parsers we explicitly whitelist above.
  (reg/register-parser-factories! {:select select-formats-whitelist
                                   :construct construct-formats-whitelist
                                   :ask ask-formats-whitelist})

  (let [query-endpoint (get-required-configuration-setting :sparql-query-endpoint config)
        update-endpoint (get-required-configuration-setting :sparql-update-endpoint config)]
    (create-sparql-repository query-endpoint update-endpoint)))

(defmethod ig/init-key :drafter.backend.sesame/remote [k opts]
  (log/info "Initialising Backend")
  (get-backend opts))


(defmethod ig/halt-key! :drafter.backend.sesame/remote [k backend]
  (log/info "Halting Backend")
  (drpr/stop-backend backend))