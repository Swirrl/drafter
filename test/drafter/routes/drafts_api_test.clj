(ns drafter.routes.drafts-api-test
  (:require [drafter.test-common :refer [*test-db* test-triples wrap-with-clean-test-db
                                         stream->string select-all-in-graph]]
            [clojure.test :refer :all]
            [grafter.rdf.sesame :as ses]
            [drafter.routes.drafts-api :refer :all]
            [clojure.java.io :as io]
            [drafter.rdf.draft-management :refer :all]))

(def test-graph-uri "http://example.org/my-graph")

(defn url? [u]
  (try
    (java.net.URL. u)
    true
    (catch Exception ex
      false)))

(defn make-live-graph [db graph-uri]
  (let [draft-graph (import-data-to-draft! db graph-uri (test-triples "http://test.com/subject-1"))]
    (migrate-live! db draft-graph)))

(defn make-live-graph-2 [db graph-uri]
  (let [draft-graph (import-data-to-draft! db graph-uri (test-triples "http://test.com/subject-2"))]
    (migrate-live! db draft-graph)))

(deftest drafts-api-routes-test

  (let [state (atom {})]

    (testing "POST /draft/create"
      (testing "without a live-graph param returns a 400 error"
        (let [{:keys [status body headers]} ((draft-api-routes "/draft" *test-db* state)
                                             {:uri "/draft/create"
                                              :request-method :post
                                              :headers {"accept" "application/json"}})]
          (is (= 400 status))
          (is (= :error (body :type)))
          (is (instance? String (body :msg)))))

      (testing (str "with live-graph=" test-graph-uri " should create a new managed graph and draft")

        (let [{:keys [status body headers]} ((draft-api-routes "/draft" *test-db* state)
                                             {:uri "/draft/create"
                                              :request-method :post
                                              :query-params {"live-graph" test-graph-uri}
                                              :headers {"accept" "application/json"}})]
          (is (= 201 status))
          (is (url? (-> body :guri)))))))

    (testing "POST /draft"
      (testing "with a file"
        (let [state (atom {})
              test-request {:uri "/draft"
                            :request-method :post
                            :query-params {"graph" "http://mygraph/graph-to-be-appended-to"}
                            :params {:file {:filename "test.nt"
                                            :tempfile (io/file "./test/test-triple.nt")
                                            :content-type "application/n-triples"
                                            :size 10}}}

              route (draft-api-routes "/draft" *test-db* state)
              {:keys [status body headers]} (route test-request)]

          (testing "returns success"
            (is (= 200 status))
            (is (= :ok (:type body))))

          (testing "appends RDF to the graph"
            (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-appended-to> {<http://example.org/test/triple> ?p ?o . }}"))))))


      (testing "with an invalid RDF file"
        (let [state (atom {})
              test-request {:uri "/draft"
                            :request-method :post
                            :query-params {"graph" "http://mygraph/graph-to-be-appended-to"}
                            :params {:file {:filename "invalid-file.nt"
                                            :tempfile (io/file "project.clj") ;; not an RDF file
                                            :content-type "application/n-triples" ;; but claim it is
                                            :size 10}}}

              route (draft-api-routes "/draft" *test-db* state)
              {:keys [status body headers]} (route test-request)]

          (is (= 400 status) "400 Bad Request")))

      (testing "with a source graph"
        ;; put some data into the source-graph before we begin
        (make-live-graph *test-db* "http://draft.org/source-graph")
          (let [state (atom {})
                test-request {:uri "/draft"
                              :request-method :post
                              :query-params {"graph" "http://mygraph/graph-to-be-appended-to" "source-graph" "http://draft.org/source-graph"}}
                route (draft-api-routes "/draft" *test-db* state)
                {:keys [status body headers]} (route test-request)]

            (testing "returns success"
              (is (= 200 status))
              (is (= :ok (:type body))))

            (testing "appends RDF to the graph"
              (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-appended-to> { <http://test.com/subject-1> ?p ?o . }}")) "graph has got new data in" )
              (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-appended-to> { <http://example.org/test/triple> ?p ?o . }}")) "graph has still got the old data in ")))))

    (testing "PUT /draft with a source file"
      (make-live-graph *test-db* "http://mygraph/graph-to-be-replaced")

      (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-replaced> { <http://test.com/subject-1> ?p ?o . } }"))
                 "Graph should contain initial state before it is replaced")

      (let [state (atom {})
            test-request {:uri "/draft"
                          :request-method :put
                          :query-params {"graph" "http://mygraph/graph-to-be-replaced"}
                          :params {:file {:filename "test.nt"
                                          :tempfile (io/file "./test/test-triple.nt")
                                          :content-type "application/n-triples"
                                          :size 10}}}
             route (draft-api-routes "/draft" *test-db* state)
             {:keys [status body headers]} (route test-request)]

        (testing "returns success"
              (is (= 200 status))
              (is (= :ok (:type body))))

        (testing "replaces RDF in the graph"
          (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-replaced> { <http://example.org/test/triple> ?p ?o . } }"))
                 "The data should be replaced with the new data"))))

        ; in a different test so that it's in a clean db.
    (testing "PUT /draft with a source graph"
      (make-live-graph *test-db* "http://mygraph/graph-to-be-replaced")
      (make-live-graph-2 *test-db* "http://draft.org/source-graph")

      (testing "when source graph contains data"

        (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-replaced> { <http://test.com/subject-1> ?p ?o . } }"))
                   "Graph should contain initial state before it is replaced")

        (let [state (atom {})
               test-request {:uri "/draft"
                             :request-method :put
                             :query-params {"graph" "http://mygraph/graph-to-be-replaced" "source-graph" "http://draft.org/source-graph"}}

               route (draft-api-routes "/draft" *test-db* state)
               {:keys [status body headers]} (route test-request)]

          (testing "returns success"
              (is (= 200 status))
              (is (= :ok (:type body))))

          (testing "replaces RDF in the graph"

            (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-replaced> { <http://test.com/subject-2> ?p ?o . } }"))
                 "The data should be replaced with the new data")))

      (testing "when source graph doesn't contain data"
        ;; what's left from the previous test.
        (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-replaced> { <http://test.com/subject-2> ?p ?o . } }"))
                     "Graph should contain initial state before it is replaced")

         (let [state (atom {})
               test-request {:uri "/draft"
                             :request-method :put
                             :query-params {"graph" "http://mygraph/graph-to-be-replaced" "source-graph" "http://draft.org/source-graph-x"}}

               route (draft-api-routes "/draft" *test-db* state)
               {:keys [status body headers]} (route test-request)]

          (testing "returns success"
              (is (= 200 status))
              (is (= :ok (:type body))))

          (testing "the job when run deletes contents of the RDF graph"
              (is (= false (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-replaced> { <http://test.com/subject-2> ?p ?o . } }")))
                    "Destination graph should be deleted")))))))

(deftest graph-management-delete-graph-test
  (testing "DELETE /graph"
      (do
        (make-live-graph *test-db* "http://mygraph/live-graph")

        (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/live-graph> { ?s ?p ?o } }"))
            "Graph should exist before deletion")

        (let [state (atom {})
              route (graph-management-routes "/graph" *test-db* state)
              test-request {:uri "/graph"
                            :request-method :delete
                            :query-params {"graph" "http://mygraph/live-graph"}}
              {:keys [status body headers]} (route test-request)]

          (testing "returns success"
            (is (= 200 status))
            (is (= :ok (:type body))))

          (testing "delete job actually deletes the graph"
            (is (not (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/live-graph> { ?s ?p ?o } }")))
                "Graph should be deleted"))))))

(deftest graph-management-live-test-with-one-graph
  (testing "PUT /graph/live"
    (let [draft-graph (import-data-to-draft! *test-db* "http://mygraph.com/live-graph" (test-triples "http://test.com/subject-1"))]
      (is (ses/query *test-db* (str "ASK WHERE { GRAPH <" draft-graph "> { <http://test.com/subject-1> ?p ?o } }"))
          "Draft graph should exist before deletion")

      (let [state (atom {})
            route (graph-management-routes "/graph" *test-db* state)

            test-request {:uri "/graph/live"
                          :request-method :put
                          :query-params {"graph" draft-graph}}

            {:keys [status body headers]} (route test-request)]

        (testing "returns success"
          (is (= 200 status))
          (is (= :ok (:type body))))

        (testing "moves the draft to live"
          (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph.com/live-graph> { <http://test.com/subject-1> ?p ?o } }"))
              "Live graph should contain our triples"))))))

(deftest graph-management-live-test-multiple-graphs
  (testing "PUT /graph/live"
    (let [draft-graph-1 (import-data-to-draft! *test-db* "http://mygraph.com/live-graph-1" (test-triples "http://test.com/subject-1"))
          draft-graph-2 (import-data-to-draft! *test-db* "http://mygraph.com/live-graph-2" (test-triples "http://test.com/subject-1"))]

      (let [state (atom {})
            route (graph-management-routes "/graph" *test-db* state)
            test-request {:uri "/graph/live"
                          :request-method :put
                          :query-params {"graph" [draft-graph-1 draft-graph-2]}}

            {:keys [status body headers]} (route test-request)]

        (testing "returns success"
          (is (= 200 status))
          (is (= :ok (:type body))))

        (testing "moves the draft to live"
          (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph.com/live-graph-1> { <http://test.com/subject-1> ?p ?o } }"))
              "Live graph should contain our triples")
          (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph.com/live-graph-2> { <http://test.com/subject-1> ?p ?o } }"))
              "Live graph should contain our triples"))))))

(use-fixtures :each wrap-with-clean-test-db)
