(ns drafter.rdf.sparql-protocol-test
  (:require
   [drafter.test-common :refer :all]
   [grafter.rdf.formats :refer [rdf-ntriples]]
   [grafter.rdf :as rdf]
   [grafter.rdf.protocols :as pr]
   [grafter.rdf.repository :refer :all]
   [drafter.rdf.sparql-protocol :refer :all]
   [clojure.java.io :as io]
   [clojure.data.json :as json]
   [clojure-csv.core :as csv]
   [clojure.test :refer :all])

  (:import [java.io ByteArrayOutputStream]
           [java.util Scanner]
           [org.openrdf.query.resultio.sparqljson SPARQLResultsJSONWriter]))

(defn add-triple-to-db [db]
  (pr/add db "http://foo.com/my-graph" (test-triples "http://test.com/data/one")))

(deftest results-streamer-test
  (testing "Streams sparql results into output stream"
    (let [baos (ByteArrayOutputStream.)
          preped-query (prepare-query *test-db* "SELECT * WHERE { ?s ?p ?o }")
          streamer! (result-streamer SPARQLResultsJSONWriter
                                     nil
                                     preped-query
                                     "application/sparql-results+json"
                                     (fn []))]

      (streamer! baos)

      (let [output (-> baos .toByteArray String. json/read-str)]
        (is (map? output))))))

(deftest sparql-end-point-test
  (let [end-point (sparql-end-point "/live/sparql" *test-db*)]
    (testing "Standard SPARQL query with no dataset restrictions"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method :get
                                     :uri "/live/sparql"
                                     :params {:query "SELECT * WHERE { ?s ?p ?o } LIMIT 10"}
                                     :headers {"accept" "text/csv"}})]

        (is (= 200 status))
        (is (= "text/csv" (headers "Content-Type")))

        (let [csv-result (csv/parse-csv (stream->string body))]
          (is (= ["s" "p" "o"] (first csv-result)))
          (is (= ["http://test.com/data/one" "http://test.com/hasProperty" "http://test.com/data/1"]
                 (second csv-result))))))))

(defn get-spo [{:keys [s p o]}]
  [s p o])

(defn get-spo-set [triples]
  (set (map (fn [{:keys [s p o]}] [s p o]) triples)))

(deftest sparql-end-point-graph-query-accept-test
  (let [end-point (sparql-end-point "/live/sparql" *test-db*)]
    (testing "Standard SPARQL query with multiple accepted MIME types and qualities"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method :get
                                     :uri "/live/sparql"
                                     :params {:query "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 10"}
                                     :headers {"accept" "text/csv;q=0.7,text/unknown,application/n-triples;q=0.9"}})]

        (is (= 200 status))
        (is (= "application/n-triples" (headers "Content-Type")))

        (let [triple-reader (java.io.InputStreamReader. body)
              triples (get-spo-set (rdf/statements triple-reader :format rdf-ntriples))
              expected-triples (get-spo-set (test-triples "http://test.com/data/one"))]

          (is (= expected-triples triples)))))))

(deftest sparql-end-point-tuple-query-accept-test
  (let [end-point (sparql-end-point "/live/sparql" *test-db*)]
    (testing "Tuple SPARQL query with multiple accepted MIME types and qualities"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method :get
                                     :uri "/live/sparql"
                                     :params {:query "SELECT * WHERE { ?s ?p ?o } LIMIT 10"}
                                     :headers {"accept" "text/csv,application/sparql-results+json;q=0.9,*/*;q=0.8"}})]

        (is (= 200 status))
        (is (= "text/csv" (headers "Content-Type")))

        (let [csv-result (csv/parse-csv (stream->string body))
              triples (set (drop 1 csv-result))
              expected-triples (get-spo-set (test-triples "http://test.com/data/one"))]
          (is (= ["s" "p" "o"] (first csv-result)))
          (is (= expected-triples triples)))))))

(deftest sparql-end-point-boolean-query-accept-test
  (let [end-point (sparql-end-point "/live/sparql" *test-db*)]
    (testing "Boolean SPARQL query with multiple accepted MIME types and qualities"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method :get
                                     :uri "/live/sparql"
                                     :params {:query "ASK WHERE { ?s ?p ?o }"}
                                     :headers {"accept" "text/plain,application/sparql-results+json;q=0.1,*/*;q=0.8"}})]

        (is (= 200 status))
        (is (= "text/plain" (headers "Content-Type")))

        (let [body-str (stream->string body)]
          (is (= "true" body-str)))))))

(deftest sparql-endpoint-sets-content-type-text-plain-if-html-requested
  (let [end-point (sparql-end-point "/live/sparql" *test-db*)]
    (testing "SPARQL endpoint sets content type to text/plain if text/html requested"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method :get
                                     :uri "/live/sparql"
                                     :params {:query "SELECT * WHERE { ?s ?p ?o }"}
                                     :headers {"accept" "text/html"}})]

        (is (= 200 status))
        (is (= "text/plain" (headers "Content-Type")))))))

(use-fixtures :each (partial wrap-with-clean-test-db
                             add-triple-to-db))
