(ns drafter.rdf.sparql-protocol-test
  (:require [clojure-csv.core :as csv]
            [clojure.test :refer :all]
            [drafter.rdf.sparql-protocol :refer :all]
            [drafter.test-common :refer :all]
            [grafter.rdf :as rdf]
            [grafter.rdf
             [formats :refer [rdf-ntriples]]
             [protocols :as pr]]
            [schema.test :refer [validate-schemas]])
  (:import [java.util.concurrent CountDownLatch TimeUnit]
           [java.net URI]))

(use-fixtures :each validate-schemas)

(defn add-triple-to-db [db]
  (pr/add db (URI. "http://foo.com/my-graph") (test-triples (URI. "http://test.com/data/one"))))

(deftest sparql-end-point-test
  (let [end-point (sparql-end-point "/live/sparql" *test-backend*)]
    (testing "Standard SPARQL query with no dataset restrictions"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method :get
                                     :uri "/live/sparql"
                                     :query-params {"query" "SELECT * WHERE { ?s ?p ?o } LIMIT 10"}
                                     :headers {"accept" "text/csv"}})]

        (is (= 200 status))
        (is (= "text/csv" (headers "Content-Type")))

        (let [csv-result (csv/parse-csv (stream->string body))]
          (is (= ["s" "p" "o"] (first csv-result)))
          (is (= ["http://test.com/data/one" "http://test.com/hasProperty" "http://test.com/data/1"]
                 (second csv-result))))))))

(defn get-spo-set [triples]
  (set (map (fn [{:keys [s p o]}] [s p o]) triples)))

(deftest sparql-end-point-graph-query-accept-test
  (let [end-point (sparql-end-point "/live/sparql" *test-backend*)]
    (testing "Standard SPARQL query with multiple accepted MIME types and qualities"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method           :get
                                                 :uri          "/live/sparql"
                                                 :query-params {"query" "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 10"}
                                                 :headers      {"accept" "text/csv;q=0.7,text/unknown,application/n-triples;q=0.9"}})]

        (is (= 200 status))
        (is (= "application/n-triples" (headers "Content-Type")))

        (let [triple-reader (java.io.InputStreamReader. body)
              triples (get-spo-set (rdf/statements triple-reader :format rdf-ntriples))
              expected-triples (get-spo-set (test-triples (URI. "http://test.com/data/one")))]

          (is (= expected-triples triples)))))))

(deftest sparql-end-point-tuple-query-accept-test
  (let [end-point (sparql-end-point "/live/sparql" *test-backend*)]
    (testing "Tuple SPARQL query with multiple accepted MIME types and qualities"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method :get
                                     :uri "/live/sparql"
                                     :query-params {"query" "SELECT * WHERE { ?s ?p ?o } LIMIT 10"}
                                     :headers {"accept" "text/csv,application/sparql-results+json;q=0.9,*/*;q=0.8"}})]

        (is (= 200 status))
        (is (= "text/csv" (headers "Content-Type")))

        (let [csv-result (csv/parse-csv (stream->string body))
              triples (set (map (fn [row] (map #(URI. %) row)) (drop 1 csv-result)))
              expected-triples (get-spo-set (test-triples (URI. "http://test.com/data/one")))]
          (is (= ["s" "p" "o"] (first csv-result)))
          (is (= expected-triples triples)))))))

(deftest sparql-end-point-boolean-query-accept-test
  (let [end-point (sparql-end-point "/live/sparql" *test-backend*)]
    (testing "Boolean SPARQL query with multiple accepted MIME types and qualities"
      (let [{:keys [status headers body]
             :as resp} (end-point {:request-method :get
                                   :uri "/live/sparql"
                                   :query-params {"query" "ASK WHERE { ?s ?p ?o }"}
                                   :headers {"accept" "text/plain,application/sparql-results+json;q=0.1,*/*;q=0.8"
                                             "Accept-Charset" "utf-8"}})]
        (is (= 200 status))
        (is (= "text/plain; charset=utf-8" (headers "Content-Type")))

        (let [body-str (stream->string body)]
          (is (= "true" body-str)))))))

(deftest sparql-endpoint-sets-content-type-text-plain-if-html-requested
  (let [end-point (sparql-end-point "/live/sparql" *test-backend*)]
    (testing "SPARQL endpoint sets content type to text/plain if text/html requested"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method :get
                                     :uri "/live/sparql"
                                     :query-params {"query" "SELECT * WHERE { ?s ?p ?o }"}
                                     :headers {"accept" "text/html"}})]

        (is (= 200 status))
        (is (= "text/plain; charset=utf-8" (headers "Content-Type")))))))

(deftest sparql-endpoint-invalid-query
  (testing "SPARQL endpoint returns client error if SPARQL query invalid"
    (let [endpoint (sparql-end-point "/live/sparql" *test-backend*)
          request {:request-method :get
                   :uri "/live/sparql"
                   :query-params {"query" "NOT A VALID SPARQL QUERY"}
                   :headers {"accept" "text/plain"}}
          {:keys [status]} (endpoint request)]
      (is (= 400 status)))))

(deftest sparql-endpoint-query-timeout
  (let [test-port 8080
        max-connections (int 2)
        connection-latch (CountDownLatch. max-connections)
        release-latch (CountDownLatch. 1)
        repo (doto (get-latched-http-server-repo test-port) (.setMaxConcurrentHttpConnections max-connections))
        endpoint (sparql-end-point "/live/sparql" repo)
        test-request {:uri "/live/sparql"
                      :request-method :get
                      :query-params {"query" "SELECT * WHERE { ?s ?p ?o }"}
                      :headers {"accept" "application/sparql-results+json"}}]
    (with-open [server (latched-http-server test-port connection-latch release-latch (get-spo-http-response))]
      (let [blocked-connections (doall (map (fn [i] (future (endpoint test-request))) (range 1 (inc max-connections))))]
        ;;wait for max number of connections to be accepted by the server
        (if (.await connection-latch 5000 TimeUnit/MILLISECONDS)
          (do
            ;;server has accepted max number of connections so next query attempt should see a connection timeout
            (let [rf (future (endpoint test-request))]
              ;;should be rejected almost immediately
              (let [response (.get rf 5000 TimeUnit/MILLISECONDS)]
                (assert-is-service-unavailable-response response)))

            ;;release previous connections and wait for them to complete
            (.countDown release-latch)
            (doseq [f blocked-connections]
              (.get f 100 TimeUnit/MILLISECONDS)))
          (throw (RuntimeException. "Server failed to accept connections within timeout")))))))

(use-fixtures :once wrap-db-setup)

(use-fixtures :each (partial wrap-clean-test-db
                             add-triple-to-db))
