(ns drafter-swagger-client.api.sparql-endpoints
  (:require [drafter-swagger-client.core :refer [call-api check-required-params with-collection-format]])
  (:import (java.io File)))

(defn sparql-live-get-with-http-info
  "Queries the published data with SPARQL
  Query the live data via the SPARQL query langauge and protocol.
Please consult the SPARQL query protocol specification http://www.w3.org/TR/sparql11-protocol/ for a description of this endpoint."
  ([query ] (sparql-live-get-with-http-info query nil))
  ([query {:keys [timeout ]}]
   (check-required-params query)
   (call-api "/sparql/live" :get
             {:path-params   {}
              :header-params {}
              :query-params  {"query" query "timeout" timeout }
              :form-params   {}
              :content-types ["application/x-www-form-urlencoded"]
              :accepts       ["application/n-triples" "application/rdf+xml" "text/turtle" "application/sparql-results+xml" "application/sparql-results+json" "text/csv"]
              :auth-names    []})))

(defn sparql-live-get
  "Queries the published data with SPARQL
  Query the live data via the SPARQL query langauge and protocol.
Please consult the SPARQL query protocol specification http://www.w3.org/TR/sparql11-protocol/ for a description of this endpoint."
  ([query ] (sparql-live-get query nil))
  ([query optional-params]
   (:data (sparql-live-get-with-http-info query optional-params))))

(defn sparql-live-post-with-http-info
  "Queries the published data with SPARQL
  Query the live data via the SPARQL query langauge and protocol.
Please consult the SPARQL query protocol specification http://www.w3.org/TR/sparql11-protocol/ for a description of this endpoint."
  [query ]
  (check-required-params query)
  (call-api "/sparql/live" :post
            {:path-params   {}
             :header-params {}
             :query-params  {}
             :form-params   {"query" query }
             :content-types ["application/x-www-form-urlencoded"]
             :accepts       ["application/n-triples" "application/rdf+xml" "text/turtle" "application/sparql-results+xml" "application/sparql-results+json" "text/csv"]
             :auth-names    []}))

(defn sparql-live-post
  "Queries the published data with SPARQL
  Query the live data via the SPARQL query langauge and protocol.
Please consult the SPARQL query protocol specification http://www.w3.org/TR/sparql11-protocol/ for a description of this endpoint."
  [query ]
  (:data (sparql-live-post-with-http-info query)))

