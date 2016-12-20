(ns drafter.backend.sesame.remote
  (:require [clojure.string :as str]
            [drafter.backend.repository]
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

(defn get-required-environment-variable [var-key env-map]
  (if-let [ev (var-key env-map)]
    ev
    (let [var-name (str/upper-case (str/replace (name var-key) \- \_))
          message (str "Missing required key "
                       var-key
                       " in environment map. Ensure you export an environment variable "
                       var-name
                       " or define "
                       var-key
                       " in the relevant profile in profiles.clj")]
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

(defn get-backend [env-map]
  "Creates a new SPARQL repository with the query and update endpoints
  configured in the given environment map."

  ;; This call here obliterates the sesame defaults for registered
  ;; parsers.  Forcing content negotiation to work only with the
  ;; parsers we explicitly whitelist above.
  (reg/register-parser-factories! {:select select-formats-whitelist
                                   :construct construct-formats-whitelist
                                   :ask ask-formats-whitelist})

  (let [query-endpoint (get-required-environment-variable :sparql-query-endpoint env-map)
        update-endpoint (get-required-environment-variable :sparql-update-endpoint env-map)]
    (create-sparql-repository query-endpoint update-endpoint)))



(comment

  ;; The code below is playing about trying to construct a custom Httpclient
  ;; configured how we want for sesame.  Unfortunately it doesn't work
  ;; yet... but maybe one day after
  ;; https://openrdf.atlassian.net/browse/SES-2368 is resolved.

  (import '[org.apache.http.impl.client HttpClients]
          '[org.openrdf.http.client SesameClient SesameSession SesameClientImpl]
          '[org.apache.http.client.config RequestConfig]
          '[java.util.concurrent Executors]
          )

  (defn build-http-client [repo]
    (let [request-config (.. (RequestConfig/custom)
                             (setConnectionRequestTimeout 1)
                             build)
          ;;sesame-client (.getSesameClient repo) ;; TODO consider making a new one

          executor (Executors/newCachedThreadPool)

          http-client (.. (HttpClients/custom)
                          useSystemProperties
                          (setUserAgent "Drafter")
                          (setMaxConnPerRoute 1)
                          (setDefaultRequestConfig request-config)
                          build)

          sesame-client (.getSesameClient repo)]

      (.setHttpClient sesame-client http-client)
      (.setSesameClient repo sesame-client)

      (prn request-config)
      (prn sesame-client)
      (prn http-client))
    repo))