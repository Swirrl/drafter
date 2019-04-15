(ns drafter-swagger-client.api.querying
  (:require [drafter-swagger-client.core :refer [call-api check-required-params with-collection-format]])
  (:import (java.io File)))

(defn draftset-id-data-get-with-http-info
  "Access the quads inside this Draftset
  Request the contents of this draftset in any supported RDF
serialisation.

If the chosen serialisation is a triple based one then the
graph query string parameter must be provided.  If a quad
based serialisation is chosen then the graph parameter must be
omitted or a 415 error will be returned."
  ([id ] (draftset-id-data-get-with-http-info id nil))
  ([id {:keys [graph union-with-live timeout ]}]
   (check-required-params id)
   (call-api "/draftset/{id}/data" :get
             {:path-params   {"id" id }
              :header-params {}
              :query-params  {"graph" graph "union-with-live" union-with-live "timeout" timeout }
              :form-params   {}
              :content-types ["application/json"]
              :accepts       (if graph
                               ["application/n-triples"]
                               ["application/n-quads"])
              :auth-names    ["jws-auth"]
              :as :stream})))

(defn draftset-id-data-get
  "Access the quads inside this Draftset
  Request the contents of this draftset in any supported RDF
serialisation.

If the chosen serialisation is a triple based one then the
graph query string parameter must be provided.  If a quad
based serialisation is chosen then the graph parameter must be
omitted or a 415 error will be returned."
  ([id ] (draftset-id-data-get id nil))
  ([id optional-params]
   (:data (draftset-id-data-get-with-http-info id optional-params))))

(defn draftset-id-query-get-with-http-info
  "Query this Draftset with SPARQL
  Query this Draftset via the SPARQL query language and protocol.

Please consult the SPARQL query protocol specification http://www.w3.org/TR/sparql11-protocol/ for a description of this endpoint."
  ([id query ] (draftset-id-query-get-with-http-info id query nil))
  ([id query {:keys [union-with-live timeout ]}]
   (check-required-params id query)
   (call-api "/draftset/{id}/query" :get
             {:path-params   {"id" id }
              :header-params {}
              :query-params  {"query" query "union-with-live" union-with-live "timeout" timeout }
              :form-params   {}
              :content-types ["application/sparql-query"]
              :accepts       ["application/n-triples" "application/rdf+xml" "text/turtle" "application/sparql-results+xml" "application/sparql-results+json" "text/csv"]
              :auth-names    ["jws-auth"]})))

(defn draftset-id-query-get
  "Query this Draftset with SPARQL
  Query this Draftset via the SPARQL query language and protocol.

Please consult the SPARQL query protocol specification http://www.w3.org/TR/sparql11-protocol/ for a description of this endpoint."
  ([id query ] (draftset-id-query-get id query nil))
  ([id query optional-params]
   (:data (draftset-id-query-get-with-http-info id query optional-params))))

(defn draftset-id-query-post-with-http-info
  "Query this Draftset with SPARQL
  Query this Draftset via the SPARQL query language and protocol.

Please consult the SPARQL query protocol specification http://www.w3.org/TR/sparql11-protocol/ for a description of this endpoint."
  ([id query ] (draftset-id-query-post-with-http-info id query nil))
  ([id query {:keys [union-with-live timeout ]}]
   (check-required-params id query)
   (call-api "/draftset/{id}/query" :post
             {:path-params   {"id" id }
              :header-params {}
              :query-params  {"union-with-live" union-with-live "timeout" timeout }
              :form-params   {"query" query }
              :content-types ["application/x-www-form-urlencoded"]
              :accepts       ["application/n-triples" "application/rdf+xml" "text/turtle" "application/sparql-results+xml" "application/sparql-results+json" "text/csv"]
              :auth-names    ["jws-auth"]})))

(defn draftset-id-query-post
  "Query this Draftset with SPARQL
  Query this Draftset via the SPARQL query language and protocol.

Please consult the SPARQL query protocol specification http://www.w3.org/TR/sparql11-protocol/ for a description of this endpoint."
  ([id query ] (draftset-id-query-post id query nil))
  ([id query optional-params]
   (:data (draftset-id-query-post-with-http-info id query optional-params))))

