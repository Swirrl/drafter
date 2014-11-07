(ns drafter.rdf.sparql-protocol-test
  (:require
   [drafter.test-common :refer :all]
   [grafter.rdf :as rdf]
   [grafter.rdf.protocols :as pr]
   [grafter.rdf.sesame :refer :all]
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
                                     "application/sparql-results+json")]

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

(deftest sparql-end-point-accept-test
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
              triples (get-spo-set (rdf/statements triple-reader :format rdf/format-rdf-ntriples))
              expected-triples (get-spo-set (test-triples "http://test.com/data/one"))]

          (is (= expected-triples triples)))))))

(use-fixtures :each (partial wrap-with-clean-test-db
                             add-triple-to-db))
