(ns drafter.rdf.sparql-protocol-test
  (:require
   [drafter.test-common :refer :all]
   [grafter.rdf.protocols :as pr]
   [grafter.rdf.sesame :refer :all]
   [drafter.rdf.sparql-protocol :refer :all]
   [clojure.java.io :as io]
   [clojure.data.json :as json]
   [clojure-csv.core :as csv]
   [clojure.test :refer :all])
  (:import [java.io ByteArrayOutputStream]
           [java.util Scanner]))

(defn add-triple-to-db [db]
  (pr/add db "http://foo.com/my-graph" (test-triples "http://test.com/data/one")))

(deftest sparql-results!-test
  (testing "Streams sparql results into output stream"
    (let [baos (ByteArrayOutputStream.)]

      (-> (prepare-query *test-db* "SELECT * WHERE { ?s ?p ?o }")
          (sparql-results! baos "application/sparql-results+json"))

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

(use-fixtures :each (partial wrap-with-clean-test-db
                             add-triple-to-db))
