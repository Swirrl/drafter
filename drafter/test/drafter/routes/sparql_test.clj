(ns drafter.routes.sparql-test
  (:require [clojure-csv.core :as csv]
            [clojure.test :as t :refer :all]
            [clojure.tools.logging :as log]
            [drafter.backend.draftset.draft-management :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.test-common :as tc :refer [select-all-in-graph stream->string]]
            [schema.test :refer [validate-schemas]])
  (:import java.net.URI))

(use-fixtures :each
  (join-fixtures [validate-schemas])
  tc/with-spec-instrumentation)

(def draft-graph-1 (draft:graph "dg-1"))
(def draft-graph-2 (draft:graph "dg-2"))

(def graph-1-result [(URI. "http://test.com/subject-1") (URI. "http://test.com/hasProperty") (URI. "http://test.com/data/1")])

(def default-sparql-query {:request-method :get
                           :uri "/sparql/live"
                           :query-params {"query" "SELECT * WHERE { ?s ?p ?o }"}
                           :headers {"accept" "text/csv"}})

(defn- build-query
  ([endpoint-path query] (build-query endpoint-path query))
  ([endpoint-path query graphs]
   (let [query-request (-> default-sparql-query
                           (assoc-in [:query-params "query"] query)
                           (assoc :uri endpoint-path))]

     (reduce (fn [req graph]
               (log/spy (update-in req [:params :graph] (fn [old new]
                                                          (cond
                                                            (nil? old) new
                                                            (instance? String old) [old new]
                                                            :else (conj old new))) graph)))
             query-request graphs))))

(defn live-query [qstr]
  (build-query "/v1/sparql/live" qstr nil))

(defn csv-> [{:keys [body]}]
  "Parse a response into a CSV"
  (-> body stream->string csv/parse-csv))

(t/deftest live-sparql-routes-test
  (tc/with-system
    [{endpoint :drafter.routes.sparql/live-sparql-query-route
      :keys    [drafter.stasher/repo] :as system} "drafter/routes/sparql-test/system.edn"]
    (let [{:keys [status headers body]
           :as   result} (endpoint (live-query (select-all-in-graph "http://test.com/made-live-and-deleted-1")))
          csv-result (csv-> result)]

      (is (= "text/csv" (headers "Content-Type"))
          "Returns content-type")

      (is (= ["s" "p" "o"] (first csv-result))
          "Returns CSV")

      (is (= graph-1-result (map #(URI. %) (second csv-result)))
          "Named (live) graph is publicly queryable")

      (testing "Draft graphs are not exposed"
        (let [csv-result (csv-> (endpoint
                                  (live-query (select-all-in-graph draft-graph-2))))]
          (is (empty? (second csv-result)))))

      (testing "Offline public graphs are not exposed"
        (set-isPublic! repo (URI. "http://test.com/made-live-and-deleted-1") false)
        (let [csv-result (csv-> (endpoint
                                  (live-query
                                    (select-all-in-graph (URI. "http://test.com/made-live-and-deleted-1")))))]
          (is (not= graph-1-result (second csv-result))))))))

(tc/deftest-system-with-keys query-draftset-disallowed-with-service-query
  [:drafter.routes.sparql/live-sparql-query-route]
  [system "drafter/routes/sparql-test/system.edn"]
  (let [handler (get system :drafter.routes.sparql/live-sparql-query-route)]
    (let [query-request (live-query "SELECT * WHERE { SERVICE <http://anything> { ?s ?p ?o } }")
          query-response (handler query-request)]
      (tc/assert-is-bad-request-response query-response))
    (let [query-request (live-query "
SELECT * WHERE {
  GRAPH ?g { ?s ?p ?o }
  GRAPH ?g {
    SERVICE <db:somedb> {
      { ?s ?p ?o }
    }
  }
}")
          query-response (handler query-request)]
      (tc/assert-is-bad-request-response query-response)
      (let [query-request (live-query "
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX sdmx: <http://purl.org/linked-data/sdmx/2009/concept#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT (COUNT(*) as ?tripod_count_var) {
  SELECT * {
    SELECT * WHERE {
      ?s ?p ?odd
    }
    LIMIT 100
  }
  LIMIT 1000 OFFSET 0
}")
            query-response (handler query-request)]
        (tc/assert-is-ok-response query-response)))))
