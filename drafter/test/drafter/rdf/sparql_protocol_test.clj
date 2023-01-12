(ns drafter.rdf.sparql-protocol-test
  (:require [clojure-csv.core :as csv]
            [clojure.test :refer :all]
            [drafter.backend.common :as bcom]
            [drafter.rdf.sparql :as sparql]
            [drafter.rdf.sparql-protocol :refer :all]
            [drafter.util :as util]
            [drafter.test-common :as tc]
            [grafter-2.rdf4j.io :as rio]
            [grafter-2.rdf4j.repository :as repo]
            [ring.util.response :as ring]
            [schema.test :refer [validate-schemas]])
  (:import java.net.URI
           [java.io ByteArrayInputStream]
           [java.util.concurrent CountDownLatch TimeUnit]))

(use-fixtures :each tc/with-spec-instrumentation)

(defn add-triple-to-db [db]
  (sparql/add db (URI. "http://foo.com/my-graph") (tc/test-triples (URI. "http://test.com/data/one"))))

(def add-triple-fixture (fn [test]
                          (add-triple-to-db tc/*test-backend*)
                          (test)))

(use-fixtures :each (join-fixtures (reverse [validate-schemas
                                             add-triple-fixture
                                             (tc/wrap-system-setup
                                              "drafter/feature/empty-db-system.edn"
                                              [:drafter.fixture-data/loader :drafter.stasher/repo :drafter/write-scheduler])])))

(deftest sparql-prepare-query-handler-test
  (let [r (repo/sail-repo)
        handler (sparql-prepare-query-handler r identity)]
    (testing "Valid query"
      (let [req (handler {:sparql {:query-string "SELECT * WHERE { ?s ?p ?o }"}})]
        (is (some? (get-in req [:sparql :prepared-query])))))

    (testing "User restricted (FROM) query: no overrides"
      (let [qs "SELECT * FROM <http://foo> FROM NAMED <http://baz> WHERE { ?s ?p ?o }"
            foo "http://foo"
            baz "http://baz"
            req (handler {:sparql {:query-string qs}})
            ds (.getActiveDataset (get-in req [:sparql :prepared-query]))]
        (is (contains? (.getDefaultGraphs ds) (util/uri->rdf4j-uri foo)))
        (is (contains? (.getNamedGraphs ds) (util/uri->rdf4j-uri baz)))))

    (testing "User restricted (FROM) query: protocol overrides query"
      (let [qs "SELECT * FROM <http://foo> WHERE { ?s ?p ?o }"
            foo "http://foo"
            bar "http://bar"
            baz "http://baz"
            req (handler {:sparql {:query-string qs
                                   :default-graph-uri [bar]
                                   :named-graph-uri [baz]}})
            ds (.getActiveDataset (get-in req [:sparql :prepared-query]))]
        (is (contains? (.getDefaultGraphs ds) (util/uri->rdf4j-uri bar)))
        (is (not (contains? (.getDefaultGraphs ds) (util/uri->rdf4j-uri foo))))
        (is (contains? (.getNamedGraphs ds) (util/uri->rdf4j-uri baz)))))

    (testing "Malformed SPARQL query"
      (let [response (handler {:sparql {:query-string "NOT A SPARQL QUERY"}})]
        (tc/assert-is-bad-request-response response)))))

(defn- prepare-query-str [query-str]
  (with-open [conn (repo/->connection (repo/sail-repo))]
    (bcom/prep-and-validate-query conn query-str)))

(deftest sparql-negotiation-handler-test
  (testing "Valid request"
    (let [handler (sparql-negotiation-handler identity)
          accept-content-type "application/n-triples"
          pquery (prepare-query-str "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
          request {:uri "/sparql"
                   :sparql {:prepared-query pquery}
                   :headers {"accept" accept-content-type}}
          {{:keys [format response-content-type]} :sparql} (handler request)]
      (is (= accept-content-type response-content-type))
      (is (some? format))))

  (testing "Content negotiation failure"
    (let [handler (sparql-negotiation-handler identity)
          pquery (prepare-query-str "SELECT * WHERE { ?s ?p ?o }")
          response (handler {:uri "/test"
                             :sparql {:prepared-query pquery}
                             :headers {"accept" "text/trig"}})]
      (tc/assert-is-not-acceptable-response response)))

  (testing "Content negotiation via query parameter"
    (let [handler (sparql-negotiation-handler identity)
          pquery (prepare-query-str "SELECT * WHERE { ?s ?p ?o }")
          request {:uri "/sparql"
                   :sparql {:prepared-query pquery}
                   :headers {"accept" "*/*"}
                   :params {:accept "application/json"}}
          {{:keys [format response-content-type]} :sparql} (handler request)]
      (is (= "application/json" response-content-type))
      (is (some? format)))))

(deftest sparql-timeout-handler-test
  (testing "With valid timeout"
    (let [timeout 30
          handler (sparql-timeout-handler (constantly timeout) identity)
          pquery (prepare-query-str "SELECT * WHERE { ?s ?p ?o }")
          request {:sparql {:prepared-query pquery}}]
      (handler request)
      (is (= timeout (.getMaxQueryTime pquery)))))

  (testing "With invalid timeout"
    (let [ex (IllegalArgumentException. "Invalid timeout")
          handler (sparql-timeout-handler (constantly ex) identity)
          pquery (prepare-query-str "SELECT * WHERE { ?s ?p ?o }")
          request {:sparql {:prepared-query pquery}}
          response (handler request)]
      (is (tc/assert-is-bad-request-response response)))))

(deftest sparql-end-point-test
  (let [end-point (sparql-end-point "/live/sparql" tc/*test-backend*)]
    (testing "Standard SPARQL query with no dataset restrictions"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method :get
                                     :uri "/live/sparql"
                                     :query-params {"query" "SELECT * WHERE { ?s ?p ?o } LIMIT 10"}
                                     :headers {"accept" "text/csv"}})]

        (is (= 200 status))
        (is (= "text/csv" (headers "Content-Type")))

        (let [csv-result (csv/parse-csv (tc/stream->string body))]
          (is (= ["s" "p" "o"] (first csv-result)))
          (is (= ["http://test.com/data/one" "http://test.com/hasProperty" "http://test.com/data/1"]
                 (second csv-result))))))

    (testing "Queries differing only by dataset restrictions are cached separately"
      (let [query (fn [query-params]
                    (end-point {:request-method :get
                                :uri "/live/sparql"
                                :query-params query-params
                                :headers {"accept" "text/csv"}}))]
        (let [res (query
                    {"query" "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 1"
                     "default-graph-uri" "http://foo.com/some-other-graph"
                     "named-graph-uri" "http://foo.com/my-graph"})]
          (is (= [["g" "s" "p" "o"]
                  ["http://foo.com/my-graph"
                   "http://test.com/data/one"
                   "http://test.com/hasProperty"
                   "http://test.com/data/1"]]
                (csv/parse-csv (tc/stream->string (:body res))))))

        (let [res (query
                    {"query" "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 1"
                     ;; Note default-graph-uri and named-graph-uri are swapped
                     ;; compared to above, so we should get no results since
                     ;; http://foo.com/some-other-graph doesn't exist.
                     "default-graph-uri" "http://foo.com/my-graph"
                     "named-graph-uri" "http://foo.com/some-other-graph"})]
          (is (= [["g" "s" "p" "o"]]
                (csv/parse-csv (tc/stream->string (:body res))))))))))

(defn get-spo-set [triples]
  (set (map (fn [{:keys [s p o]}] [s p o]) triples)))

(deftest sparql-end-point-graph-query-accept-test
  (let [end-point (sparql-end-point "/live/sparql" tc/*test-backend*)]
    (testing "Standard SPARQL query with multiple accepted MIME types and qualities"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method           :get
                                                 :uri          "/live/sparql"
                                                 :query-params {"query" "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 10"}
                                                 :headers      {"accept" "text/csv;q=0.7,text/unknown,application/n-triples;q=0.9"}})]

        (is (= 200 status))
        (is (= "application/n-triples" (headers "Content-Type")))

        (let [triple-reader (java.io.InputStreamReader. body)
              triples (get-spo-set (rio/statements triple-reader :format :nt))
              expected-triples (get-spo-set (tc/test-triples (URI. "http://test.com/data/one")))]

          (is (= expected-triples triples)))))))

(deftest sparql-end-point-tuple-query-accept-test
  (let [end-point (sparql-end-point "/live/sparql" tc/*test-backend*)]
    (testing "Tuple SPARQL query with multiple accepted MIME types and qualities"
      (let [{:keys [status headers body]
             :as result} (end-point {:request-method :get
                                     :uri "/live/sparql"
                                     :query-params {"query" "SELECT * WHERE { ?s ?p ?o } LIMIT 10"}
                                     :headers {"accept" "text/csv,application/sparql-results+json;q=0.9,*/*;q=0.8"}})]

        (is (= 200 status))
        (is (= "text/csv" (headers "Content-Type")))

        (let [csv-result (csv/parse-csv (tc/stream->string body))
              triples (set (map (fn [row] (map #(URI. %) row)) (drop 1 csv-result)))
              expected-triples (get-spo-set (tc/test-triples (URI. "http://test.com/data/one")))]
          (is (= ["s" "p" "o"] (first csv-result)))
          (is (= expected-triples triples)))))))

(deftest sparql-end-point-boolean-query-accept-test
  (let [end-point (sparql-end-point "/live/sparql" tc/*test-backend*)]
    (testing "Boolean SPARQL query with multiple accepted MIME types and qualities"
      (let [{:keys [status headers body]
             :as resp} (end-point {:request-method :get
                                   :uri "/live/sparql"
                                   :query-params {"query" "ASK WHERE { ?s ?p ?o }"}
                                   :headers {"accept" "text/plain,application/sparql-results+json;q=0.1,*/*;q=0.8"
                                             "Accept-Charset" "utf-8"}})]
        (is (= 200 status))
        (is (= "text/plain; charset=utf-8" (headers "Content-Type")))

        (let [body-str (tc/stream->string body)]
          (is (= "true" body-str)))))))

(deftest sparql-endpoint-sets-content-type-text-plain-if-html-requested
  (let [end-point (sparql-end-point "/live/sparql" tc/*test-backend*)]
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
    (let [endpoint (sparql-end-point "/live/sparql" tc/*test-backend*)
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
        repo (tc/concurrent-test-repo test-port max-connections)
        endpoint (sut/sparql-end-point "/live/sparql" repo)
        test-request {:uri "/live/sparql"
                      :request-method :get
                      :query-params {"query" "SELECT * WHERE { ?s ?p ?o }"}
                      :headers {"accept" "application/sparql-results+json"}}]
    (with-open [server (tc/latched-http-server test-port connection-latch release-latch (tc/get-spo-http-response))]
      (let [blocked-connections (doall (map (fn [i] (future (endpoint test-request))) (range 1 (inc max-connections))))]
        ;;wait for max number of connections to be accepted by the server
        (if (.await connection-latch 5000 TimeUnit/MILLISECONDS)
          (do
            ;;server has accepted max number of connections so next query attempt should see a connection timeout
            (let [rf (future (endpoint test-request))]
              ;;should be rejected almost immediately
              (let [response (.get rf 5000 TimeUnit/MILLISECONDS)]
                (tc/assert-is-service-unavailable-response response)))

            ;;release previous connections and wait for them to complete
            (.countDown release-latch)
            (doseq [f blocked-connections]
              (.get f 100 TimeUnit/MILLISECONDS)))
          (throw (RuntimeException. "Server failed to accept connections within timeout")))))))

(deftest sparql-query-parser-handler-test
  (let [handler (sparql-query-parser-handler identity)
        query-string "SELECT * WHERE { ?s ?p ?o }"]
    (testing "Valid GET request"
      (let [req {:request-method :get :query-params {"query" query-string}}
            inner-req (handler req)]
        (is (= query-string (get-in inner-req [:sparql :query-string])))))

    (testing "Invalid GET request with missing query parameter"
      (let [resp (handler {:request-method :get :query-params {}})]
        (tc/assert-is-unprocessable-response resp)))

    (testing "Invalid GET request with multiple 'query' query parameters"
      (let [req {:request-method :get
                 :query-params {"query" [query-string "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"]}}
            resp (handler req)]
        (tc/assert-is-unprocessable-response resp)))

    (testing "Valid form POST request"
      (let [req {:request-method :post
                 :headers {"content-type" "application/x-www-form-urlencoded"}
                 :form-params {"query" query-string}}
            inner-req (handler req)]
        (is (= query-string (get-in inner-req [:sparql :query-string])))))

    (testing "Invalid form POST request with missing query form parameter"
      (let [req {:request-method :post
                 :headers {"content-type" "application/x-www-form-urlencoded"}
                 :form-params {}}
            resp (handler req)]
        (tc/assert-is-unprocessable-response resp)))

    (testing "Invalid form POST request with multiple query form parameters"
      (let [req {:request-method :post
                 :headers {"content-type" "application/x-www-form-urlencoded"}
                 :form-params {"query" [query-string "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"]}}
            resp (handler req)]
        (tc/assert-is-unprocessable-response resp)))

    (testing "Valid body POST request"
      (let [req {:request-method :post
                 :headers {"content-type" "application/sparql-query"}
                 :body (char-array query-string)}
            inner-req (handler req)]
        (is (= query-string (get-in inner-req [:sparql :query-string])))))

    (testing "POST request with invalid content type"
      (let [req {:request-method :post
                 :headers {"content-type" "text/plain"}
                 :params {:query query-string}}
            inner-req (handler req)]
        (is (= query-string (get-in inner-req [:sparql :query-string])))))

    (testing "Invalid request method"
      (let [req {:request-method :put :body query-string}
            resp (handler req)]
        (tc/assert-is-method-not-allowed-response resp)))))
