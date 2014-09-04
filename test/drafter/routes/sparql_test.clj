(ns drafter.routes.sparql-test
  (:require [drafter.test-common :refer [test-triples
                                         stream->string select-all-in-graph]]
            [clojure.test :refer :all]
            [clojure-csv.core :as csv]
            [grafter.rdf.sesame :as ses]
            [drafter.routes.sparql :refer :all]
            [drafter.rdf.sparql-rewriting :refer [function-registry register-function]]
            [drafter.rdf.draft-management :refer :all]))

(defn add-test-data!
  "Set the state of the database so that we have three managed graphs,
  one of which is made public the other two are still private (draft)."
  [db]
  (let [draft-made-live-and-deleted (import-data-to-draft! db "http://test.com/graph-1" (test-triples "http://test.com/subject-1"))
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
(comment (deftest live-sparql-routes-test
           (let [[draft-graph-1 draft-graph-2] (add-test-data! *test-db*)
                 endpoint (live-sparql-routes *test-db*)
                 {:keys [status headers body]
                  :as result} (endpoint
                               (assoc-in default-sparql-query [:params :query]
                                         (select-all-in-graph "http://test.com/graph-1")))
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
               (set-isPublic! *test-db* "http://test.com/graph-1" false)
               (let [csv-result (csv-> (endpoint
                                        (assoc-in default-sparql-query [:params :query]
                                                  (select-all-in-graph "http://test.com/graph-1"))))]

                 (is (not= graph-1-result (second csv-result)))))))

         (deftest state-sparql-routes-test
           (let [drafts-request (-> default-sparql-query
                                    (assoc :uri "/sparql/state")
                                    (assoc-in [:headers "accept"] "text/plain"))
                 [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! *test-db*)
                 endpoint (state-sparql-routes *test-db*)]

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

                 (is (= "false" body)))))))


(deftest drafts-sparql-routes-test
  (let [*test-db* (ses/repo)
        drafts-request (assoc default-sparql-query :uri "/sparql/draft")
        [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! *test-db*)
        endpoint (draft-sparql-routes "/sparql/draft" *test-db*)]

    (testing "draft graphs that are made live can no longer be queried on their draft GURI"
      (let [csv-result (csv-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:query-params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query] (select-all-in-graph draft-graph-2)))))]

        (is (= graph-2-result (second csv-result)))))

    (testing "Can query draft :graphs that are supplied on the request"
      (let [csv-result (csv-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:query-params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query] (select-all-in-graph draft-graph-2)))))]

        (is (= graph-2-result (second csv-result))
            "The data should be as expected")
        (is (= 3 (count csv-result))
            "There should be two results and one header row"))

      ;; TODO test rewriting of results
      (testing "Can query union of several specified draft graphs"
        (let [csv-result (csv-> (endpoint
                                 (-> drafts-request
                                     (assoc-in [:query-params "graph"] [draft-graph-2 draft-graph-3])
                                     (assoc-in [:params :query] "SELECT * WHERE { ?s ?p ?o }"))))]

          (is (= 5 (count csv-result))
              "There should be 5 results (2 triples in both graphs + the csv header row)")))

      (testing "Can rewrite queries to use their draft"
        (let [csv-result (csv-> (endpoint
                                 (-> drafts-request
                                     (assoc-in [:query-params "graph"] [draft-graph-2])
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
              "There should be 5 results (2 triples in both graphs + the csv header row)"))))

    (testing "Can do a construct query"
      (let [csv-result (csv-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:params :query] "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"))))]
        (= graph-2-result (second csv-result))))))

(deftest drafts-sparql-route-rewrites-constants
  (let [db (ses/repo)
        drafts-request (assoc default-sparql-query :uri "/sparql/draft")
        [_ draft-graph-2 draft-graph-3] (add-test-data! db)
        endpoint (draft-sparql-routes "/sparql/draft" db)]

    (register-function drafter.rdf.sparql-rewriting/function-registry
                       "http://publishmydata.com/def/functions#replace-live-graph-uri"
                       (partial drafter.rdf.draft-management/draft-graphs db))

    (testing "Query constants in graph position are rewritten to their draft graph URI"
        (let [s-p-o-result (-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:query-params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query]
                                             "SELECT * WHERE { GRAPH <http://test.com/graph-2> { ?s ?p ?o . } } LIMIT 1")))
                              csv->
                              second)]

          (is (= ["http://test.com/subject-2" "http://test.com/hasProperty" "http://test.com/data/1"]
                 s-p-o-result))))))

(deftest drafts-sparql-routes-with-results-rewriting-test
  (let [db (ses/repo)
        drafts-request (assoc default-sparql-query :uri "/sparql/draft")
        [_ draft-graph-2 draft-graph-3] (add-test-data! db)
        endpoint (draft-sparql-routes "/sparql/draft" db)]

    ;; register the function that does the results rewriting
    (register-function drafter.rdf.sparql-rewriting/function-registry
                       "http://publishmydata.com/def/functions#replace-live-graph-uri"
                       (partial drafter.rdf.draft-management/lookup-draft-graph-uri db))

    (testing "Queries can be written against their live graph URI"
        (let [found-graph (-> (endpoint
                               (-> drafts-request
                                   (assoc-in [:query-params "graph"] [draft-graph-2])
                                   (assoc-in [:params :query]
                                             "SELECT ?g ?s ?p ?o WHERE { BIND(URI(\"http://test.com/graph-2\") AS ?g) GRAPH ?g { ?s ?p ?o . } } LIMIT 1")))
                              csv->
                              second
                              first ;; ?g is the first result
                              )]

          ;;(is (= nil (ses/query db (str "SELECT * WHERE { ?live <" drafter.rdf.drafter-ontology/drafter:hasDraft "> ?draft . }"))))

          (is (= "http://test.com/graph-2" found-graph))))))

(deftest error-on-invalid-context
  (let [db (ses/repo)
        drafts-request (assoc default-sparql-query :uri "/sparql/draft")
        draft-one (import-data-to-draft! db "http://test.com/graph-1" (test-triples "http://test.com/subject-1"))
        draft-two (import-data-to-draft! db "http://test.com/graph-1" (test-triples "http://test.com/subject-1"))
        endpoint (draft-sparql-routes "/sparql/draft" db)]

    (testing "When the context is set to two drafts which represent the same live graph an error should be raised."
      (let [{:keys [status headers body] :as result} (-> (endpoint
                                                          (-> drafts-request
                                                              (assoc-in [:query-params "graph"] [draft-one draft-two])
                                                              (assoc-in [:params :query]
                                                                        "SELECT * WHERE { ?s ?p ?o . } LIMIT 1"))))]

        (is (= 412 status))
        ;; TODO write unit test

        ))))



;;(use-fixtures :each (partial wrap-with-clean-test-db))
