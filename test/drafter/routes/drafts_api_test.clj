(ns drafter.routes.drafts-api-test
  (:require [drafter.test-common :refer [*test-db* test-triples wrap-with-clean-test-db
                                         make-store stream->string select-all-in-graph make-graph-live!]]
            [clojure.test :refer :all]
            [grafter.rdf.repository :as repo]
            [drafter.routes.drafts-api :refer :all]
            [drafter.rdf.drafter-ontology :refer [meta-uri]]
            [clojure.java.io :as io]
            [clojure.template :refer [do-template]]
            [drafter.rdf.draft-management :refer :all]
            [drafter.write-scheduler :refer [finished-jobs restart-id]])
  (:import [java.util UUID]))

(def test-graph-uri "http://example.org/my-graph")

(defn url? [u]
  (try
    (java.net.URL. u)
    true
    (catch Exception ex
      false)))

(defn get-file-request-data [path]
  (let [file (io/file path)]
    {:tempfile file
     :filename (.getName file)
     :size (.length file)
     :content-type "application/n-triples"}))

(defn add-request-file-data [request data]
  (assoc-in request [:params :file] data))

(defn add-request-file [request path]
  (add-request-file-data request (get-file-request-data path)))

(defn add-request-metadata [request meta-name value]
  (assoc-in request [:query-params (str "meta-" meta-name)] value))

(defn add-request-graph [request graph]
  (assoc-in request [:params :graph] graph))

(defn add-request-source-graph [request source-graph]
  (assoc-in request [:params :source-graph] source-graph))

(defn add-request-graph-source-graph [request dest-graph source-graph]
  (-> request
      (add-request-graph dest-graph)
      (add-request-source-graph source-graph)))

(defn add-request-graph-source-file [request dest-graph source-file]
  (-> request
      (add-request-graph dest-graph)
      (add-request-file source-file)))

(defn is-created [response]
  (testing "returns 201 created"
    (let [{:keys [status body headers]} response]
      (is (= 201 status))
      (is (url? (-> body :guri))))))

(defn is-success [response]
  (let [{:keys [status body headers]} response]
    (is (= 200 status))
    (is (= :ok (:type body)))))

(def job-id-path #"/status/finished-jobs/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})")

(defn parse-guid [job-path]
  (if-let [uid (second (re-matches job-id-path job-path))]
    (UUID/fromString uid)))

(defn job-is-accepted [response]
  (let [{:keys [status body headers]} response]
    (testing "returns 202 - Job accepted"
      (is (= 202 status))
      (is (= :ok (:type body)))
      (is (= restart-id (:restart-id body)))
      (is (instance? UUID (parse-guid (:finished-job body)))))))

(defn is-error-response [response]
  (let [{:keys [status body headers]} response]
    (is (= 400 status))
    (is (= :error (body :type)))
    (is (instance? String (body :msg)))))

(deftest drafts-api-create-draft-test
  (testing "POST /draft/create"
    (testing "without a live-graph param returns a 400 error"
      (let [response ((draft-api-routes "/draft" *test-db*)
                                           {:uri "/draft/create"
                                            :request-method :post
                                            :headers {"accept" "application/json"}})]
        (is-error-response response)))

    (testing (str "with live-graph=" test-graph-uri " should create a new managed graph and draft")

      (let [{:keys [status body headers] :as response} ((draft-api-routes "/draft" *test-db*)
                                                        {:uri "/draft/create"
                                                         :request-method :post
                                                         :params {:live-graph test-graph-uri}
                                                         :headers {"accept" "application/json"}})]
        (is-created response)))

    (testing (str "with meta data" test-graph-uri " should see meta data stored")

      (let [{:keys [status body headers]} ((draft-api-routes "/draft" *test-db*)
                                           {:uri "/draft/create"
                                            :request-method :post
                                            :params {:live-graph test-graph-uri "meta-foo" "foo" "meta-bar" "bar"}
                                            :headers {"accept" "application/json"}})]
        (is (= 201 status))
        (is (url? (-> body :guri)))
        (is (repo/query *test-db* (str "ASK WHERE {"
                                       "  GRAPH <" drafter-state-graph "> {"
                                       "     ?graph <" (meta-uri "foo") "> ?foo ."
                                       "     ?graph <" (meta-uri "bar") "> ?bar ."
                                       "  } "
                                       "}"))
            "meta-* params are converted into metadata predicates.")))))

(def default-timeout 5000)

(defn await-completion
  "Test helper to await for an async operation to complete.  Takes the
  state atom a GUID for the job id and waits until timeout for the job
  to appear.

  If the job doesn't appear before timeout time has passed an
  exception is raised."
  ([state-atom path] (await-completion state-atom path default-timeout))
  ([state-atom path timeout]
   (let [start (System/currentTimeMillis)]
     (loop [state-atom state-atom
            guid (parse-guid path)]
       (if-let [value (@state-atom guid)]
         @value
         (if (> (System/currentTimeMillis) (+ start (or timeout default-timeout) ))
           (throw (RuntimeException. "Timed out awaiting test value"))
           (do
             (Thread/sleep 5)
             (recur state-atom guid))))))))

(def ok-response {:status 200 :headers {"Content-Type" "application/json"} :body {:type :ok}})

(deftest drafts-api-routes-test
  (testing "POST /draft"
    (testing "with a file"
      (let [dest-graph "http://mygraph/graph-to-be-appended-to"
            test-request (-> {:uri "/draft" :request-method :post}
                             (add-request-graph-source-file dest-graph "./test/test-triple.nt"))

            route (draft-api-routes "/draft" *test-db*)
            {:keys [status body headers] :as response} (route test-request)]

        (job-is-accepted response)
        (is (= ok-response
               (await-completion finished-jobs (:finished-job body))))

        (testing "appends RDF to the graph"
          (is (repo/query *test-db* (str "ASK WHERE { GRAPH <" dest-graph "> {<http://example.org/test/triple> ?p ?o . }}"))))))

    (testing "with an invalid RDF file"
      (let [test-request (-> {:uri "/draft" :request-method :post}
                             ;; project.clj is not RDF
                             (add-request-graph-source-file "http://mygraph/graph-to-be-appended-to" "project.clj"))

            route (draft-api-routes "/draft" *test-db*)
            {:keys [body] :as response} (route test-request)]

        (job-is-accepted response)

        (testing "Stores ParseException in DONE atom of responses for later collection via REST API"
          (let [guid (:finished-job body)
                error-result (await-completion finished-jobs guid)]

            (is (= :error (:type error-result)))
            (is (instance? org.openrdf.rio.RDFParseException (:exception error-result)))))))))

(deftest graph-management-delete-graph-test
  (testing "DELETE /graph"
      (do
        (make-graph-live! *test-db* "http://mygraph/live-graph")

        (is (repo/query *test-db* "ASK WHERE { GRAPH <http://mygraph/live-graph> { ?s ?p ?o } }")
            "Graph should exist before deletion")

        (let [route (graph-management-routes "/graph" *test-db*)
              test-request (-> {:uri "/graph" :request-method :delete}
                               (add-request-graph "http://mygraph/live-graph"))
              response (route test-request)]

          (job-is-accepted response)
          (await-completion finished-jobs (:finished-job (:body response)))

          (testing "delete job actually deletes the graph"
            (is (not (repo/query *test-db* "ASK WHERE { GRAPH <http://mygraph/live-graph> { ?s ?p ?o } }"))
                "Graph should be deleted"))))))

(deftest graph-management-live-test-with-one-graph
  (testing "PUT /graph/live"
    (let [draft-graph (import-data-to-draft! *test-db* "http://mygraph.com/live-graph" (test-triples "http://test.com/subject-1"))]
      (is (repo/query *test-db* (str "ASK WHERE { GRAPH <" draft-graph "> { <http://test.com/subject-1> ?p ?o } }"))
          "Draft graph should exist before deletion")

      (let [route (graph-management-routes "/graph" *test-db*)
            test-request (-> {:uri "/graph/live" :request-method :put
                              :params {:graph draft-graph}})

            {:keys [body] :as response} (route test-request)]

        (job-is-accepted response)
        (await-completion finished-jobs (:finished-job body))

        (testing "moves the draft to live"
          (is (repo/query *test-db* "ASK WHERE { GRAPH <http://mygraph.com/live-graph> { <http://test.com/subject-1> ?p ?o } }")
              "Live graph should contain our triples"))))))

(deftest graph-management-live-test-multiple-graphs
  (testing "PUT /graph/live"
    (let [draft-graph-1 (import-data-to-draft! *test-db* "http://mygraph.com/live-graph-1" (test-triples "http://test.com/subject-1"))
          draft-graph-2 (import-data-to-draft! *test-db* "http://mygraph.com/live-graph-2" (test-triples "http://test.com/subject-1"))]

      (let [route (graph-management-routes "/graph" *test-db*)
            test-request {:uri "/graph/live"
                          :request-method :put
                          :params {:graph [draft-graph-1 draft-graph-2]}}

            {:keys [status body headers] :as response} (route test-request)]

        (job-is-accepted response)
        (await-completion finished-jobs (:finished-job body))

        (testing "moves the draft to live"
          (is (repo/query *test-db* "ASK WHERE { GRAPH <http://mygraph.com/live-graph-1> { <http://test.com/subject-1> ?p ?o } }")
              "Live graph should contain our triples")
          (is (repo/query *test-db* "ASK WHERE { GRAPH <http://mygraph.com/live-graph-2> { <http://test.com/subject-1> ?p ?o } }")
              "Live graph should contain our triples"))))))

(defn metadata-exists-sparql [draft-graph-uri name value]
  (let [meta-subject (meta-uri name)]
    (str "ASK WHERE { GRAPH <" drafter-state-graph "> { <" draft-graph-uri "> <" meta-subject "> \"" value "\" } }")))

(defn metadata-values-sparql [draft-graph-uri name]
  (let [meta-subject (meta-uri name)]
    (str "SELECT ?o WHERE { GRAPH <" drafter-state-graph "> {
           <" draft-graph-uri "> <" meta-subject "> ?o }"
        "}")))

(do-template
 [test-name http-method modify-request]

 (deftest test-name
   (testing "Updating draft graph with metadata"
     (testing "Adds metadata"
       (let [route (draft-api-routes "/draft" *test-db*)
             source-graph-uri (make-graph-live! *test-db* "http://mygraph/source-graph")
             draft-graph-uri (create-draft-graph! *test-db* "http://mygraph/dest-graph")

             request (-> {:uri "/draft" :request-method http-method}
                         (add-request-metadata "uploaded-by" "fido")
                         (modify-request draft-graph-uri))

             {:keys [status body]} (route request)]

         (testing "Accepts job"
           (is (= 202 status)))

         (await-completion finished-jobs (:finished-job body))

         (let [sparql (metadata-exists-sparql draft-graph-uri "uploaded-by" "fido")]
           (is (repo/query *test-db* sparql)))

         (testing "Overwrites metadata"
           ;; try overwriting the value we've just written i.e. replacing
           ;; {uploaded-by fido} with {uploaded-by bonzo}

           (let [new-request (add-request-metadata request "uploaded-by" "bonzo")
                 {:keys [status body]} (route new-request)]

             (testing "Accepts job"
               (is (= 202 status)))

             (await-completion finished-jobs (:finished-job body))

             (let [meta-query (metadata-values-sparql draft-graph-uri "uploaded-by")
                   meta-records (repo/query *test-db* meta-query)]
               (testing "Metadata overwritten"
                 (is (= 1 (count meta-records))
                     (= "updated" (get (first meta-records) "o")))))))))))

 meta-update-with-post-file-test :post (fn [req graph] (add-request-graph-source-file req graph "./test/test-triple.nt")))

(use-fixtures :each wrap-with-clean-test-db)
