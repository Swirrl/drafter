(ns drafter.backend.sesame.remote.repository
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [grafter.rdf.repository.registry :as reg])
  (:import [drafter.rdf DrafterSPARQLRepository]
           [java.nio.charset Charset]
           [org.openrdf.rio RDFParserRegistry RDFFormat]
           [org.openrdf.query.resultio TupleQueryResultFormat BooleanQueryResultFormat
            TupleQueryResultParserRegistry
            BooleanQueryResultParserRegistry]
           [org.openrdf.query.resultio.text.csv SPARQLResultsCSVParserFactory]))

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

(defn create-repository-for-environment [env-map]
  "Creates a new SPARQL repository with the query and update endpoints
  configured in the given environment map."

  (let [updated-registries (update (reg/registered-parser-factories)
                                   ;; Force removal of TurtleParser
                                   :construct #(disj % org.openrdf.rio.turtle.TurtleParserFactory))]
    (reg/register-parser-factories! updated-registries))

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
    repo)

  )
