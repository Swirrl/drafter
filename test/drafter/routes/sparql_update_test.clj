(ns drafter.routes.sparql-update-test
  (:require [drafter.routes.sparql-update :refer :all]
            [drafter.rdf.draft-management :refer [create-managed-graph!]]
            [clojure.test :refer :all]
            [ring.util.codec :as codec]
            [drafter.test-common :refer [stream->string select-all-in-graph]]
            [grafter.rdf.sesame :refer [repo query]])

  (:import [java.nio.charset StandardCharsets]
           [java.io ByteArrayInputStream]))

(defn ->input-stream [s]
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
         (assoc base-request :params { "graphs" graphs })
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
         (assoc-in base-req [:form-params "graphs"] graphs)
         base-req))))

(deftest application-sparql-update-test
  (let [db (repo)
        endpoint (update-endpoint-route "/update" db)]

    (testing "POST /update"
      (testing "with a valid SPARQL update"
        (let [{:keys [status body headers]} (endpoint (application-sparql-update-request))]
          (is (= 200 status)
              "returns 200 success")
          (is (query db "ASK { <http://test/> <http://test/> <http://test/> . }")
              "Inserts the data"))))))

(deftest live-update-endpoint-route-test
  (let [db (repo)
        endpoint (live-update-endpoint-route "/update" db)]

    (create-managed-graph! db "http://example.com/")
    (testing "and a graph restriction"
      (let [request (application-sparql-update-request "INSERT { GRAPH <http://example.com/> {
                                                          <http://test/> <http://test/> <http://test/> .
                                                        }} WHERE { }")
            {:keys [status body headers]} (endpoint request)]
        (is (= 200 status)
            "returns 200 success")

        (is (query db "ASK { <http://test/> <http://test/> <http://test/> . }"
                   :default-graph ["http://example.com/"]
                   :union-graphs ["http://example.com/"])
            "Inserts the data")))))

(deftest application-x-form-urlencoded-test
  (let [db (repo)
        endpoint (update-endpoint-route "/update" db)]

    (testing "POST /update"
      (testing "with a SPARQL update"
        (let [{:keys [status body headers]} (endpoint (x-form-urlencoded-update-request))]
          (is (= 200 status)
              "returns 200 success")
          (is (query db "ASK { <http://test/> <http://test/> <http://test/> . }")
              "Inserts the data"))

        ))))


(deftest sparql-update-rewriting-test)
