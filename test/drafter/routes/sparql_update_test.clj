(ns drafter.routes.sparql-update-test
  (:require [drafter.routes.sparql-update :refer :all]
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

(defn update-request
  ([]
     (update-request default-update-string))
  ([update-str]
     {:request-method :post
      :uri "/update"
      :body (->input-stream update-str)
      :headers {:content-type "application/sparql-update"}
      :params {;; TODO :graphs
               }}))



(deftest application-sparql-update-test
  (let [db (repo)
        endpoint (update-endpoint-route "/update" db)
        {:keys [status body headers]} (endpoint (update-request))]

    (testing "POST /update"
      (testing "A successful update"
        (is (= 200 status)
            "returns 200 success")
        (is (query db "ASK { <http://test/> <http://test/> <http://test/> . }")
            "inserts the data")))))

(deftest application-x-form-urlencoded-test
  )


(deftest sparql-update-rewriting-test)
