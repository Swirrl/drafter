(ns drafter.routes.sparql-update-test
  (:require [drafter.routes.sparql-update :refer :all]
            [drafter.rdf.draft-management :refer [create-managed-graph! create-draft-graph!]]
            [drafter.write-scheduler :as scheduler]
            [clojure.test :refer :all]
            [clojure.template :refer [do-template]]
            [ring.util.codec :as codec]
            [drafter.write-scheduler :refer [global-writes-lock]]
            [drafter.test-common :refer [*test-db* wrap-with-clean-test-db stream->string select-all-in-graph make-store]]
            [grafter.rdf.repository :refer [query]])
  (:import [java.util UUID]
           [java.util.concurrent CountDownLatch TimeUnit]
           [java.nio.charset StandardCharsets]
           [java.io ByteArrayInputStream]))

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

(defn wait-for-lock-ms [lock period-ms]
  (if (.tryLock lock period-ms (TimeUnit/MILLISECONDS))
          (.unlock lock)
          (throw (RuntimeException. (str "Lock not released after " period-ms "ms")))))

(deftest application-sparql-update-test
  (let [endpoint (update-endpoint "/update" *test-db*)]

    (testing "POST /update"
      (testing "with a valid SPARQL update"
        (let [{:keys [status body headers]} (endpoint (application-sparql-update-request))]
          (is (= 200 status)
              "returns 200 success")
          (is (query *test-db* "ASK { <http://test/> <http://test/> <http://test/> . }")
              "Inserts the data"))))))

(deftest update-unavailable-test
  (do-template
   [endpoint]
   (testing "Update not available if exclusive write job running"
      (let [p (promise)
            latch (CountDownLatch. 1)
            exclusive-job (scheduler/create-job :exclusive-write (fn [j]
                                                                   (.countDown latch)
                                                                   @p))]
        
        ;submit exclusive job which should prevent updates from being scheduled
        (scheduler/submit-job! exclusive-job (UUID/randomUUID))

        ;wait until exclusive job is actually running (i.e. the write lock has been taken)
        (.await latch)

        (let [{:keys [status]} (endpoint (application-sparql-update-request))]
          (is (= 503 status)))

        ;complete exclusive job
        (deliver p nil)

        ;wait a short time for the lock to be released
        (wait-for-lock-ms global-writes-lock 200)

        ;should be able to submit updates again
        (let [{:keys [status]} (endpoint (application-sparql-update-request))]
          (is (= 200 status)))))

   ;endpoints
   (update-endpoint "/update" *test-db*)
   (draft-update-endpoint-route "/update" *test-db*)))

(deftest application-x-form-urlencoded-test
  (let [endpoint (update-endpoint "/update" *test-db*)]

    (testing "POST /update"
      (testing "with a SPARQL update"
        (let [{:keys [status body headers]} (endpoint (x-form-urlencoded-update-request))]
          (is (= 200 status)
              "returns 200 success")
          (is (query *test-db* "ASK { <http://test/> <http://test/> <http://test/> . }")
              "Inserts the data"))))))

(deftest live-update-endpoint-route-test
  (let [endpoint (live-update-endpoint-route "/update" *test-db*)]

    (create-managed-graph! *test-db* "http://example.com/")
    (testing "and a graph restriction"
      (let [request (application-sparql-update-request "INSERT { GRAPH <http://example.com/> {
                                                          <http://test/> <http://test/> <http://test/> .
                                                        }} WHERE { }")
            {:keys [status body headers]} (endpoint request)]
        (is (= 200 status)
            "returns 200 success")

        (is (query *test-db* "ASK { <http://test/> <http://test/> <http://test/> . }"
                   :default-graph ["http://example.com/"]
                   :union-graphs ["http://example.com/"])
            "Inserts the data")))))

(deftest draft-endpoint-test
  (let [endpoint (draft-update-endpoint-route "/update" *test-db*)
        live-graph (create-managed-graph! *test-db* "http://example.com/")
        draft-graph (create-draft-graph! *test-db* live-graph)]

    (testing "POST /update?graph=<draft>"
      (testing "against live graphs are stored against their draft"
        (let [{:keys [status headers body]} (endpoint (x-form-urlencoded-update-request default-update-string
                                                                                        [draft-graph]))]

          (is (= 200 status) "Returns ok")
          (is (query *test-db* (str "ASK { "
                             "  GRAPH <" draft-graph "> {"
                             "    <http://test/> <http://test/> <http://test/> ."
                             "  }"
                             "}")))))

      (testing "dynamically rewrite to their draft"
        (let [update "INSERT {
                        GRAPH ?g {
                          ?s ?p ?o .
                        }
                      } WHERE {
                        BIND(URI(\"http://example.com/\") AS ?g)
                        GRAPH ?g {
                          ?s ?p ?o .
                        }
                      }"
              {:keys [status headers body]} (endpoint (x-form-urlencoded-update-request update
                                                                                        [draft-graph]))]

          (is (= 200 status) "Returns ok")
          (is (query *test-db* (str "ASK { "
                             "  GRAPH <" draft-graph "> {"
                             "    <http://test/> <http://test/> <http://test/> ."
                             "  }"
                             "}"))))))))

(use-fixtures :each wrap-with-clean-test-db)
