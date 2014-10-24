(ns drafter.routes.sparql-test
  (:require [drafter.test-common :refer [test-triples
                                         make-store stream->string select-all-in-graph]]
            [clojure.test :refer :all]
            [clojure-csv.core :as csv]
            [clojure.data.json :as json]
            [grafter.rdf.sesame :as ses]
            [drafter.routes.sparql :refer :all]
            [drafter.rdf.sparql-rewriting :refer [function-registry register-function]]
            [drafter.rdf.draft-management :refer :all]))

(defn add-test-data!
  "Set the state of the database so that we have three managed graphs,
  one of which is made public the other two are still private (draft)."
  [db]
  (let [draft-made-live-and-deleted (import-data-to-draft! db "http://test.com/made-live-and-deleted-1" (test-triples "http://test.com/subject-1"))
        draft-2 (import-data-to-draft! db "http://test.com/graph-2" (test-triples "http://test.com/subject-2"))
        draft-3 (import-data-to-draft! db "http://test.com/graph-3" (test-triples "http://test.com/subject-3"))]
    (migrate-live! db draft-made-live-and-deleted)
    [draft-made-live-and-deleted draft-2 draft-3]))

(def graph-1-result ["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/1"])
(def graph-2-result ["http://test.com/subject-2" "http://test.com/hasProperty" "http://test.com/data/1"])

(def default-sparql-query {:request-method :get
                           :uri "/sparql/live"
                           :params {:query "SELECT * WHERE { ?s ?p ?o }"}
                           :headers {"accept" "text/csv"}})

(defn csv-> [{:keys [body]}]
  "Parse a response into a CSV"
  (-> body stream->string csv/parse-csv))

;; TODO uncomment these as soon as I get the draft one working again
(deftest live-sparql-routes-test
  (let [test-db (make-store)
        [draft-graph-1 draft-graph-2] (add-test-data! test-db)
        endpoint (live-sparql-routes "/sparql/live" test-db)
        {:keys [status headers body]
         :as result} (endpoint
         (assoc-in default-sparql-query [:params :query]
                   (select-all-in-graph "http://test.com/made-live-and-deleted-1")))
        csv-result (csv-> result)]

    (is (= "text/csv" (headers "Content-Type"))
        "Returns content-type")

    (is (= ["s" "p" "o"] (first csv-result))
        "Returns CSV")

    (is (= graph-1-result (second csv-result))
        "Named (live) graph is publicly queryable")

    (testing "Draft graphs are not exposed"
      (let [csv-result (csv-> (endpoint
                               (assoc-in default-sparql-query [:params :query]
                                         (select-all-in-graph draft-graph-2))))]
        (is (empty? (second csv-result)))))

    (testing "Offline public graphs are not exposed"
      (set-isPublic! test-db "http://test.com/made-live-and-deleted-1" false)
      (let [csv-result (csv-> (endpoint
                               (assoc-in default-sparql-query [:params :query]
                                         (select-all-in-graph "http://test.com/made-live-and-deleted-1"))))]

        (is (not= graph-1-result (second csv-result)))))))

(deftest state-sparql-routes-test
  (let [test-db (make-store)
        drafts-request (-> default-sparql-query
                           (assoc :uri "/sparql/state")
                           (assoc-in [:headers "accept"] "text/plain"))
        [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! test-db)
        endpoint (state-sparql-routes "/sparql/state" test-db)]

    (testing "The state graph should be accessible"
      (let [result (endpoint
                    (-> drafts-request
                        (assoc-in [:params :query] (str "ASK WHERE {"
                                                        "  GRAPH <" drafter-state-graph "> {"
                                                        "    ?s ?p ?o ."
                                                        "  }"
                                                        "}"))))
            body (-> result :body stream->string)]

        (is (= "true" body))))

    (testing "The data graphs (live and drafts) should be hidden"
      (let [result (endpoint
                    (-> drafts-request
                        (assoc-in [:params :query] (str "ASK WHERE {"
                                                        "  GRAPH <" draft-graph-2 "> {"
                                                        "    ?s ?p ?o ."
                                                        "  }"
                                                        "}"))))
            body (-> result :body stream->string)]

        (is (= "false" body))))))

(def drafts-request (assoc default-sparql-query :uri "/sparql/draft"))

(deftest drafts-sparql-routes-test
  (let [test-db (make-store)
        [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! test-db)
        endpoint (draft-sparql-routes "/sparql/draft" test-db)]

    (testing "draft graphs that are made live can no longer be queried on their draft GURI"
      (let [csv-result (csv-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query] (select-all-in-graph draft-graph-2)))))]

        (is (= graph-2-result (second csv-result)))))

    (testing "Can query draft :graphs that are supplied on the request"
      (let [csv-result (csv-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query] (select-all-in-graph draft-graph-2)))))]

        (is (= graph-2-result (second csv-result))
            "The data should be as expected")
        (is (= 3 (count csv-result))
            "There should be two results and one header row"))

      (testing "SELECT DISTINCT queries on drafts"
        (let [csv-result (csv-> (endpoint
                                 (-> drafts-request
                                     (assoc-in [:params "graph"] [draft-graph-2])
                                     (assoc-in [:params :query] (str "SELECT DISTINCT ?g WHERE {
                                                                        GRAPH ?g {
                                                                          ?s ?p ?o .
                                                                        }
                                                                     }")))))]
          (let [[header & results] csv-result]
            (is (= 1 (count results))
                "There should only be 1 DISTINCT ?g result returned"))))

      ;; TODO test rewriting of results
      (testing "Can query union of several specified draft graphs"
        (let [csv-result (csv-> (endpoint
                                 (-> drafts-request
                                     (assoc-in [:params "graph"] [draft-graph-2 draft-graph-3])
                                     (assoc-in [:params :query] "SELECT * WHERE { ?s ?p ?o }"))))]

          (is (= 5 (count csv-result))
              "There should be 5 results (2 triples in both graphs + the csv header row)")))

      (testing "Can rewrite queries to use their draft"
        (let [csv-result (csv-> (endpoint
                                 (-> drafts-request
                                     (assoc-in [:params "graph"] [draft-graph-2])
                                     (assoc-in [:params :query] (select-all-in-graph "http://test.com/graph-2")))))]


          (is (= graph-2-result (second csv-result))
              "The data should be as expected")

          (is (= 3 (count csv-result))
              "There should be two results and one header row")))

      (testing "If no drafts are supplied queries should be restricted to the set of live graphs"
        (let [csv-result (csv-> (endpoint
                                 (-> drafts-request
                                     (assoc-in [:params :query] "SELECT * WHERE { ?s ?p ?o }"))))]

          (= graph-1-result (second csv-result))
          (is (= 3 (count csv-result))
              "There should be 5 results (2 triples in both graphs + the csv header row)"))))))


(deftest drafts-sparql-routes-distinct-test
  (let [test-db (make-store)
        [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! test-db)
        endpoint (draft-sparql-routes "/sparql/draft" test-db)]

    (testing "SELECT DISTINCT queries on drafts"
      (let [csv-result (csv-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query] (str "SELECT DISTINCT ?g WHERE {
                                                                        GRAPH ?g {
                                                                          ?s ?p ?o .
                                                                        }
                                                                     }")))))]
        (let [[header & results] csv-result]
          (is (= 1 (count results))
              "There should only be 1 DISTINCT ?g result returned"))))))


(deftest drafts-sparql-routes-distinct-subselect-test
  (let [test-db (make-store)
        [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! test-db)
        endpoint (draft-sparql-routes "/sparql/draft" test-db)]

    (testing "SELECT DISTINCT subqueries with count on drafts"
      (let [count-request (-> drafts-request
                              (assoc-in [:params "graph"] [draft-graph-2])
                              (assoc-in [:params :query] (str "SELECT (COUNT(*) as ?tripod_count_var) {
                                                                   SELECT DISTINCT ?uri ?graph WHERE {
                                                                      GRAPH ?graph {
                                                                        ?uri ?p ?o .
                                                                       }
                                                                   }
                                                                 }")))]
        (testing "as text/csv"
          (let [{status :status headers :headers :as response} (endpoint count-request)]

            (is (= 200 status))

            (let [[header & results] (csv-> response)]
              (is (= "1" (ffirst results))
                  "There should be a count of 1 returned"))))

        (testing "as application/sparql-results+json"
          (let [count-request-as-json (assoc-in count-request [:headers "accept"] "application/sparql-results+json")
                {status :status headers :headers :as response} (endpoint count-request-as-json)]

            (println count-request-as-json)
            (is (= 200 status))

            (is (= "1" (-> response :body stream->string json/read-str
                           (get-in ["results" "bindings" 0 "tripod_count_var" "value"])))
                "There should be a count of 1 returned")))))))

(deftest drafts-sparql-routes-with-construct-queries-test
  (let [test-db (make-store)
        [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! test-db)
        endpoint (draft-sparql-routes "/sparql/draft" test-db)]

    (testing "Can do a construct query without a graph"
      (let [csv-result (csv-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:params :query] "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"))))]
        (is (= graph-1-result
               (second csv-result)))))

    (testing "Can do a construct query with against a draft graph (with query rewrite)"
      (let [csv-result (csv-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query] "CONSTRUCT { ?s ?p ?o } WHERE { graph <http://test.com/graph-2> { ?s ?p ?o . } }"))))]
        (is (= graph-2-result
               (second csv-result)))))

    (testing "Can do a construct query with a graph variable bound into results (with query & result rewrite)"
      (let [csv-result (csv-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query] "CONSTRUCT { ?g ?p ?o } WHERE { graph ?g { ?s ?p ?o . } }"))))]
        (is (= ["http://test.com/graph-2" "http://test.com/hasProperty" "http://test.com/data/1"]
               (second csv-result)))))))

(deftest drafts-sparql-routes-with-desribe-queries-test
  (let [test-db (make-store)
        [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! test-db)
        endpoint (draft-sparql-routes "/sparql/draft" test-db)]
    (testing "Can do a describe query with a graph"
      (let [csv-result (csv-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:headers "accept"] "application/n-triples")
                                   (assoc-in [:params :query] "DESCRIBE ?s WHERE { graph <http://test.com/graph-2> { ?s ?p ?o . } }"))))]
        (is (= "<http://test.com/subject-1> <http://test.com/hasProperty> <http://test.com/data/1> ."
               (-> csv-result first first)))))))

(deftest drafts-sparql-route-rewrites-constants
  (let [db (make-store)
        [_ draft-graph-2 draft-graph-3] (add-test-data! db)
        endpoint (draft-sparql-routes "/sparql/draft" db)]

    (testing "Query constants in graph position are rewritten to their draft graph URI"
        (let [s-p-o-result (-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query]
                                             "SELECT * WHERE { GRAPH <http://test.com/graph-2> { ?s ?p ?o . } } LIMIT 1")))
                              csv->
                              second)]

          (is (= ["http://test.com/subject-2" "http://test.com/hasProperty" "http://test.com/data/1"]
                 s-p-o-result))))))

(deftest drafts-sparql-routes-with-results-rewriting-test
  (let [db (make-store)
        [_ draft-graph-2 draft-graph-3] (add-test-data! db)
        endpoint (draft-sparql-routes "/sparql/draft" db)]

    (testing "Queries can be written against their live graph URI"
        (let [found-graph (-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query]
                                             "SELECT ?g ?s ?p ?o WHERE { BIND(URI(\"http://test.com/graph-2\") AS ?g) GRAPH ?g { ?s ?p ?o . } } LIMIT 1")))
                              csv->
                              second
                              first ;; ?g is the first result
                              )]

          (is (= "http://test.com/graph-2" found-graph))))))

(deftest error-on-invalid-context
  (let [db (make-store)
        drafts-request (assoc default-sparql-query :uri "/sparql/draft")
        draft-one (import-data-to-draft! db "http://test.com/made-live-and-deleted-1" (test-triples "http://test.com/subject-1"))
        draft-two (import-data-to-draft! db "http://test.com/made-live-and-deleted-1" (test-triples "http://test.com/subject-1"))
        endpoint (draft-sparql-routes "/sparql/draft" db)]

    (testing "When the context is set to two drafts which represent the same live graph an error should be raised."
      (let [{:keys [status headers body] :as result} (-> (endpoint
                                                          (-> drafts-request
                                                              (assoc-in [:params "graph"] [draft-one draft-two])
                                                              (assoc-in [:params :query]
                                                                        "SELECT * WHERE { ?s ?p ?o . } LIMIT 1"))))]

        (is (= 412 status))
        ;; TODO write unit test

        ))))


;;(use-fixtures :each (partial wrap-with-clean-test-db))
