(ns drafter.routes.api-test
  (:require [drafter.test-common :refer [*test-db* test-triples wrap-with-clean-test-db
                                         stream->string select-all-in-graph]]
            [clojure.test :refer :all]
            [grafter.rdf.sesame :as ses]
            [drafter.routes.api :refer :all]
            [drafter.rdf.queue :as q]
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


(deftest api-routes-test
  (let [q-size 1
        queue (q/make-queue q-size)]

    (testing "POST /draft/create"
      (testing "without a live-graph param returns a 400 error"
        (let [{:keys [status body headers]} ((api-routes *test-db* queue)
                                             {:uri "/draft/create"
                                              :request-method :post
                                              :headers {"accept" "application/json"}})]
          (is (= 400 status))
          (is (= :error (body :type)))
          (is (instance? String (body :msg)))))

      (testing (str "with live-graph=" test-graph-uri " should create a new managed graph and draft")

        (let [{:keys [status body headers]} ((api-routes *test-db* queue)
                                             {:uri "/draft/create"
                                              :request-method :post
                                              :query-params {"live-graph" test-graph-uri}
                                              :headers {"accept" "application/json"}})]
          (is (= 201 status))
          (is (url? (-> body :guri))))))

    (testing "POST /draft"
      (let [test-request {:uri "/draft"
                          :request-method :post
                          :query-params {"graph" "http://draft.org/draft-graph"}
                          :params {:file {:filename "test.nt"
                                          :tempfile (io/file "./test/test-triple.nt")
                                          :size 10}}}

            route (api-routes *test-db* queue)
            {:keys [status body headers]} (route test-request)]

        (testing "returns job id"
          (is (= 202 status))
          (is (instance? java.util.UUID (:queue-id body)))
          (is (instance? String (:msg body)))
          (is (= :ok (:type body))))

        (testing "adds append job to queue"
          (is (= 1 (q/size queue)))

          (let [job-id (:queue-id body)
                job-f (q/find-job queue job-id)]

            (is (fn? job-f)
                "Job function is put on the queue")

            (testing "The job when run appends RDF to the graph"
              (job-f)
              (is (ses/query *test-db* (str "ASK WHERE { <http://example.org/test/triple> ?p ?o . }"))))))

        (testing "full queue returns a 503 service temporarily unavailable"
          ;; Make a second request.  We should now 503 because the
          ;; queue is full as q-size == 1.
          (let [{:keys [status body headers]} (route test-request)]
            (is (= 503 status))
            (is (= :error (:type body)))
            (is (instance? String (:msg body))))))))

  (testing "PUT /draft"
    (make-live-graph *test-db* "http://mygraph/graph-to-be-replaced")
    (let [queue (q/make-queue 10)
          test-request {:uri "/draft"
                        :request-method :put
                        :query-params {"graph" "http://mygraph/graph-to-be-replaced"}
                        :params {:file {:filename "test.nt"
                                        :tempfile (io/file "./test/test-triple.nt")
                                        :size 10}}}

          route (api-routes *test-db* queue)
          {:keys [status body headers]} (route test-request)]

      (testing "adds replace job to queue"
        (let [job-id (:queue-id body)
              job-f (q/find-job queue job-id)]

          (is (fn? job-f))

          (testing "the job when run replaces the RDF graph"
            (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-replaced> { <http://test.com/subject-1> ?p ?o . } }"))
                "Graph should contain initial state before it is replaced")
            (job-f)
            (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/graph-to-be-replaced> { <http://example.org/test/triple> ?p ?o . } }"))
                "The data should be replaced with the new data"))))))

  (testing "DELETE /graph"
    (do
      (make-live-graph *test-db* "http://mygraph/live-graph")

      (let [queue (q/make-queue 2)
            route (api-routes *test-db* queue)

            test-request {:uri "/graph"
                          :request-method :delete
                          :query-params {"graph" "http://mygraph/live-graph"}}

            {:keys [status body headers]} (route test-request)]

        (testing "returns job id"
          (is (= 202 status))
          (is (instance? java.util.UUID (:queue-id body)))
          (is (instance? String (:msg body)))
          (is (= :ok (:type body))))

        (testing "adds a delete job to the queue"
        (let [job-id (:queue-id body)
              delete-job (q/find-job queue job-id)]

          (is (fn? delete-job))

          (testing "delete job actually deletes the graph"
            (is (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/live-graph> { ?s ?p ?o } }"))
                "Graph should exist before deletion")
            (delete-job)

            (is (not (ses/query *test-db* (str "ASK WHERE { GRAPH <http://mygraph/live-graph> { ?s ?p ?o } }")))
                "Graph should be deleted")))))))

  (testing "PUT /live"

    ;; TODO add tests for migrate

    (testing "migrates a graph from draft to live"
      )))

(use-fixtures :each wrap-with-clean-test-db)
