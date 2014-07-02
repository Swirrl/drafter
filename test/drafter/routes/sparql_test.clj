(ns drafter.routes.sparql-test
  (:require [drafter.test-common :refer [*test-db* test-triples wrap-with-clean-test-db
                                         stream->string sparql-query-request select-all-in-graph]]
            [clojure.test :refer :all]
            [clojure-csv.core :as csv]
            [grafter.rdf.sesame :as ses]
            [drafter.routes.sparql :refer :all]
            [drafter.rdf.sesame :refer :all]))

(defn add-test-data!
  "Set the state of the database so that we have three managed graphs,
  one of which is made public the other two are still private (draft)."
  [db]
  (let [draft-1 (import-data-to-draft! db "http://test.com/graph-1" (test-triples "http://test.com/subject-1"))
        draft-2 (import-data-to-draft! db "http://test.com/graph-2" (test-triples "http://test.com/subject-2"))
        draft-3 (import-data-to-draft! db "http://test.com/graph-3" (test-triples "http://test.com/subject-2"))]
    (migrate-live! db draft-1)
    [draft-1 draft-2 draft-3]))

(def graph-1-result ["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/1"])
(def graph-2-result ["http://test.com/subject-2" "http://test.com/hasProperty" "http://test.com/data/1"])

(def default-sparql-query {:request-method :get
                           :uri "/sparql/live"
                           :params {:query "SELECT * WHERE { ?s ?p ?o }"}
                           :headers {"accept" "text/csv"}})

(defn ->csv [{:keys [body]}]
  "Parse a response into a CSV"
  (-> body stream->string csv/parse-csv))

(deftest live-sparql-routes-test
  (let [[draft-graph-1 draft-graph-2] (add-test-data! *test-db*)
        endpoint (live-sparql-routes *test-db*)
        {:keys [status headers body]
         :as result} (endpoint
                      (assoc-in default-sparql-query [:params :query]
                                (select-all-in-graph "http://test.com/graph-1")))
         csv-result (->csv result)]
    (is (= "text/csv" (headers "Content-Type"))
        "Returns content-type")

    (is (= ["s" "p" "o"] (first csv-result))
        "Returns CSV")

    (is (= graph-1-result (second csv-result))
        "Named (live) graph is publicly queryable")

    (testing "Draft graphs are not exposed"
      (let [csv-result (->csv (endpoint
                               (sparql-query-request "/sparql/live"
                                                     (select-all-in-graph draft-graph-2))))]
        (is (empty? (second csv-result)))))

    (testing "Offline public graphs are not exposed"
      (set-isPublic! *test-db* "http://test.com/graph-1" false)
      (let [csv-result (->csv (endpoint
                     (sparql-query-request "/sparql/live"
                                           (select-all-in-graph "http://test.com/graph-1"))))]

        (is (not= graph-1-result (second csv-result)))))))

(deftest state-sparql-routes-test
  )

(deftest drafts-sparql-routes-test
  (let [drafts-request (assoc default-sparql-query :uri "/sparql/draft")
        [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! *test-db*)
        endpoint (draft-sparql-routes *test-db*)]

    (testing "draft graphs that are made live can no longer be queried on their draft GURI"
      (let [csv-result (->csv (endpoint
                               (-> drafts-request
                                   (assoc-in [:query-params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query] (select-all-in-graph draft-graph-2)))))]

        (is (= graph-2-result (second csv-result)))))

    (testing "Can query draft :graphs that are supplied on the request"
      (let [csv-result (->csv (endpoint
                               (-> drafts-request
                                   (assoc-in [:query-params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query] (select-all-in-graph draft-graph-2)))))]

        (is (= graph-2-result (second csv-result))
            "The data should be as expected")
        (is (= 3 (count csv-result))
            "There should be two results and one header row"))

      (testing "Can query union of several specified draft graphs"
        (let [csv-result (->csv (endpoint
                                 (-> drafts-request
                                     (assoc-in [:query-params "graph"] [draft-graph-2 draft-graph-3])
                                     (assoc-in [:params :query] "SELECT * WHERE { ?s ?p ?o }"))))]

          (is (= 5 (count csv-result))
              "There should be 5 results (2 triples in both graphs + the csv header row)")))

      (testing "If no drafts are supplied queries should be restricted to the set of live graphs"
        (let [csv-result (->csv (endpoint
                                 (-> drafts-request
                                     (assoc-in [:params :query] "SELECT * WHERE { ?s ?p ?o }"))))]

          (= graph-1-result (second csv-result))
          (is (= 3 (count csv-result))
              "There should be 5 results (2 triples in both graphs + the csv header row)"))))))

(use-fixtures :each (partial wrap-with-clean-test-db))
