(ns drafter.routes.sparql-update-test
  (:require [drafter.routes.sparql-update :refer :all]
            [drafter.test-common :refer [throws-exception?]]
            [drafter.rdf.draft-management :refer [create-managed-graph! create-draft-graph!]]
            [clojure.test :refer :all]
            [clojure.template :refer [do-template]]
            [drafter.test-common :refer [*test-backend* wrap-db-setup wrap-clean-test-db stream->string
                                         select-all-in-graph during-exclusive-write assert-is-ok-response]]
            [grafter.rdf.repository :refer [query]]
            [swirrl-server.errors :refer [encode-error]])
  (:import [java.nio.charset StandardCharsets]))

(defn ->input-stream
  "Convert a string into an Input Stream"
  [s]
  (-> s (.getBytes StandardCharsets/UTF_8) java.io.ByteArrayInputStream.))

(def default-update-string "INSERT { GRAPH <http://example.com/> {
                                      <http://test/> <http://test/> <http://test/> .
                            }} WHERE { }")

(defn application-sparql-update-request
  "Generates an update request of kind application/sparql-update."
  ([]
     (application-sparql-update-request default-update-string))
  ([update-str]
     (application-sparql-update-request update-str nil))
  ([update-str graphs]
     (let [base-request {:request-method :post
                         :uri "/update"
                         :body (->input-stream update-str)
                         :content-type "application/sparql-update"
                         }]
       (if (seq graphs)
         (assoc base-request :params { "graph" graphs })
         base-request))))

(defn x-form-urlencoded-update-request
  "Generates an update request of kind application/x-www-form-urlencoded."
  ([]
     (x-form-urlencoded-update-request default-update-string nil))
  ([update-str graphs]
     (let [base-req {:request-method :post
                     :uri "/update"
                     :form-params {"update" update-str }
                     :content-type "application/x-www-form-urlencoded"}]
       (if (seq graphs)
         (assoc-in base-req [:form-params "graph"] graphs)
         base-req))))

(deftest application-sparql-update-test
  (let [endpoint (update-endpoint "/update" *test-backend*)]

    (testing "POST /update"
      (testing "with a valid SPARQL update"
        (let [response (endpoint (application-sparql-update-request))]
          (assert-is-ok-response response)
          (is (query *test-backend* "ASK { <http://test/> <http://test/> <http://test/> . }")
              "Inserts the data"))))))

(deftest update-unavailable-test
  (testing "Update not available if exclusive write job running"
    (let [endpoint (update-endpoint "/update" *test-backend*)]

      ;; update should fail while exclusive write is in progress

      (during-exclusive-write
       (testing "An exception that encode-error's as a 503 is thrown"
         (throws-exception?
          (endpoint (application-sparql-update-request))
          (catch clojure.lang.ExceptionInfo ex
            (is (= 503 (:status (encode-error ex)))
                "The exception that is thrown encodes a 503 response")))))

      ;; should be able to submit updates again
      (let [response (endpoint (application-sparql-update-request))]
        (assert-is-ok-response response)))))

(deftest application-x-form-urlencoded-test
  (let [endpoint (update-endpoint "/update" *test-backend*)]
    (testing "POST /update"
      (testing "with a SPARQL update"
        (let [response (endpoint (x-form-urlencoded-update-request))]
          (assert-is-ok-response response)
          (is (query *test-backend* "ASK { <http://test/> <http://test/> <http://test/> . }")
              "Inserts the data"))))))

(deftest live-update-endpoint-route-test
  (let [endpoint (live-update-endpoint-route "/update" *test-backend* nil)]
    (create-managed-graph! *test-backend* "http://example.com/")
    (testing "and a graph restriction"
      (let [request (application-sparql-update-request "INSERT { GRAPH <http://example.com/> {
                                                          <http://test/> <http://test/> <http://test/> .
                                                        }} WHERE { }")
            response (endpoint request)]
        (assert-is-ok-response response)

        (is (query *test-backend* "ASK { <http://test/> <http://test/> <http://test/> . }"
                   :default-graph ["http://example.com/"]
                   :union-graphs ["http://example.com/"])
            "Inserts the data")))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
