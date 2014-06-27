(ns drafter.rdf.sparql-protocol-test
  (:require
   [grafter.rdf.protocols :as pr]
   [grafter.rdf :refer :all]
   [grafter.rdf.sesame :refer :all]
   [drafter.rdf.sparql-protocol :refer :all]
   [clojure.java.io :as io]
   [drafter.rdf.sesame-test :refer [test-triples]]
   [clojure.data.json :as json]
   [clojure.test :refer :all])
  (:import [java.io ByteArrayOutputStream]))

;; as all these tests are query only we can initialise the test db
;; once.

(def db (let [db (repo (memory-store))]
          (pr/add db "http://foo.com/" test-triples)
          db))

(deftest sparql-results!-test
  (testing "Streams sparql results into output stream"
    (let [baos (ByteArrayOutputStream.)]

      (-> (prepare-query db "SELECT * WHERE { ?s ?p ?o }")
          (sparql-results! baos "application/sparql-results+json"))

      (let [output (-> baos .toByteArray String. json/read-str)]
        (is (map? output))))))

(deftest sparql-end-point-test
  (let [end-point (sparql-end-point "/live/sparql" db)]
    (testing "Standard SPARQL query"
      (let [{:keys [status headers body] :as result}
            (end-point {:request-method :get
                        :uri "/live/sparql"
                        :params {:query "SELECT * WHERE { ?s ?p ?o } LIMIT 10"}
                        :headers {"accept" "text/csv"}})]

        (is (= 200 status))
        (is (= "text/csv" (headers "Content-Type")))
        ))))
