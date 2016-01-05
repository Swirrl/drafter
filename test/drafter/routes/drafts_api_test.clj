(ns drafter.routes.drafts-api-test
  (:require [drafter.test-common :refer [*test-backend* test-triples wrap-clean-test-db wrap-db-setup
                                         stream->string select-all-in-graph make-graph-live!
                                         import-data-to-draft!]]
            [swirrl-server.async.jobs :refer [finished-jobs]]
            [clojure.test :refer :all]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :refer [->Triple map->Triple]]
            [drafter.routes.drafts-api :refer :all]
            [drafter.rdf.drafter-ontology :refer [meta-uri] :as ontology]
            [grafter.rdf :refer [s add add-statement statements context]]
            [grafter.rdf.templater :refer [graph triplify]]
            [clojure.java.io :as io]
            [clojure.template :refer [do-template]]
            [drafter.rdf.draft-management :refer :all]
            [drafter.rdf.draft-management.jobs :refer [batched-write-size]]
            [swirrl-server.async.jobs :refer [restart-id]]
            [drafter.util :refer [set-var-root! map-values]]
            [clojure.tools.logging :as log])
  (:import [java.util UUID]
           [org.openrdf.model.impl URIImpl]))

(def test-graph-uri "http://example.org/my-graph")

(def test-triples-4 (triplify ["http://test.com/data/one"
                             ["http://test.com/hasProperty" "http://test.com/data/1"]
                             ["http://test.com/hasProperty" "http://test.com/data/2"]]

                            ["http://test.com/data/two"
                             ["http://test.com/hasProperty" "http://test.com/data/1"]
                             ["http://test.com/hasProperty" "http://test.com/data/2"]]))

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
  (assoc-in request [:params (str "meta-" meta-name)] value))

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

(defn is-client-error-response [response]
  (let [{:keys [status body headers]} response]
    (is (= 400 status))
    (is (= :error (body :type)))
    (is (instance? String (body :message)))))

(deftest drafts-api-create-draft-test
  (testing "POST /draft/create"
    (testing "without a live-graph param returns a 400 error"
      (let [response ((draft-api-routes "/draft" *test-backend*)
                      {:uri "/draft/create"
                       :request-method :post
                       :headers {"accept" "application/json"}})]
        (is-client-error-response response)))

    (testing (str "with live-graph=" test-graph-uri " should create a new managed graph and draft")

      (let [{:keys [status body headers] :as response} ((draft-api-routes "/draft" *test-backend*)
                                                        {:uri "/draft/create"
                                                         :request-method :post
                                                         :params {:live-graph test-graph-uri}
                                                         :headers {"accept" "application/json"}})]
        (is-created response)))

    (testing (str "with live-graph=" drafter-state-graph " is forbidden")
      (let [{:keys [status body headers] :as response} ((draft-api-routes "/draft" *test-backend*)
                                                        {:uri "/draft/create"
                                                         :request-method :post
                                                         :params {:live-graph drafter-state-graph}
                                                         :headers {"accept" "application/json"}})]
        (is (= 403 status))))

    (testing (str "with meta data" test-graph-uri " should see meta data stored")

      (let [{:keys [status body headers]} ((draft-api-routes "/draft" *test-backend*)
                                           {:uri "/draft/create"
                                            :request-method :post
                                            :params {:live-graph test-graph-uri "meta-foo" "foo" "meta-bar" "bar"}
                                            :headers {"accept" "application/json"}})]
        (is (= 201 status))
        (is (url? (-> body :guri)))
        (is (repo/query *test-backend* (str "ASK WHERE {"
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

(defmacro await-success
  "Waits for the job with the given path to be present in the given
  job state atom and then asserts the job succeeded. Returns the job
  result map."
  [state-atom job-path]
  `(let [job-result# (await-completion ~state-atom ~job-path)]
     (is (= :ok (:type job-result#)) (str "job failed: " (:exception job-result#)))
     job-result#))

(defn- is-client-error-response? [{:keys [status] :as response}]
  (and (>= status 400)
       (< status 500)))

(defn- append-to-draftset-request [mount-point draftset-id file-part]
  {:uri (str mount-point "/" draftset-id "/data")
   :request-method :post
   :params {:file file-part}})

(defn- create-draftset-request
  ([mount-point display-name] (create-draftset-request mount-point display-name nil))
  ([mount-point display-name description]
   (let [base-params {:display-name display-name}
         params (if (some? description) (assoc base-params :description description) base-params)]
     {:uri mount-point :request-method :post :params params})))

(defn- make-append-data-to-draftset-request [route draftset-endpoint-uri data-file-path]
  (with-open [fs (io/input-stream data-file-path)]
    (let [file-part {:tempfile fs :filename "test-dataset.trig" :content-type "application/x-trig"}
          request {:uri (str draftset-endpoint-uri "/data") :request-method :post :params {:file file-part}}]
      (route request))))

(deftest draftset-api-routes-test
  (let [mount-point "/draftset"
        route (draftset-api-routes mount-point *test-backend*)]

    (testing "Get all draftsets"
      (let [titles (map #(str "Title" %) (range 1 11))
            create-requests (map #(create-draftset-request mount-point %) titles)
            create-responses (doall (map route create-requests))]
        (doseq [r create-responses]
          (is (= 303 (:status r))))

        (let [get-all-request {:uri (str mount-point "/all") :request-method :get}
              {:keys [status body]} (route get-all-request)]
          (is (= 200 status))
          (is (= 10 (count body))))))

    (testing "Create draftset with title"
      (let [{:keys [status headers]} (route {:uri mount-point :request-method :post :params {:display-name "Test title!"}})]
        (is (= 303 status))
        (is (contains? headers "Location"))))

    (testing "Create draftset without title"
      (let [{:keys [status body]} (route {:uri mount-point :request-method :post})]
        (is (= 406 status))))

    (testing "Get non-existent draftset"
      (let [{:keys [status body]} (route {:uri (str mount-point "/missing") :request-method :get})]
        (is (= 404 status))))

    (testing "Get empty draftset without description"
      (let [display-name "Test title!"
            create-request (create-draftset-request mount-point display-name)
            create-response (route create-request)]
        (is (= 303 (:status create-response)))

        (let [draftset-location (get-in create-response [:headers "Location"])
              get-request {:uri draftset-location :request-method :get}
              {:keys [status body]} (route get-request)]
          (is (= 200 status))
          (is (contains? body :id))
          (is (= display-name (:display-name body)))
          (is (contains? body :created-at))
          (is (not (contains? body :description))))))

    (testing "Get empty draftset with description"
      (let [display-name "Test title!"
            description "Draftset used in a test"
            create-request (create-draftset-request mount-point display-name description)
            create-response (route create-request)]
        (is (= 303 (:status create-response)))

        (let [draftset-location (get-in create-response [:headers "Location"])
              get-request {:uri draftset-location :request-method :get}
              {:keys [status body]} (route get-request)]
          (is (= 200 status))
          (is (contains? body :id))
          (is (= display-name (:display-name body)))
          (is (contains? body :created-at))
          (is (= description (:description body))))))

    (testing "Get draftset containing data"
      (let [display-name "Test title!"
            create-request (create-draftset-request mount-point display-name)
            {create-status :status {draftset-location "Location"} :headers} (route create-request)
            quads (statements "test/resources/test-draftset.trig")
            live-graphs (set (keys (group-by context quads)))]
        (is (= 303 create-status))
        (let [append-response (make-append-data-to-draftset-request route draftset-location "test/resources/test-draftset.trig")]
          (await-success finished-jobs (get-in append-response [:body :finished-job]))
          (let [get-request {:uri draftset-location :request-method :get}
                {:keys [status body]} (route get-request)]
            (is (= 200 status))
            (is (contains? body :id))
            (is (= display-name (:display-name body)))
            (is (contains? body :created-at))
            (is (not (contains? body :description)))
            (is (= live-graphs (set (keys (:data body)))))))))

    (testing "Appending data to draftset"
      (testing "Quad data with valid content type for file part"
        (let [data-file-path "test/resources/test-draftset.trig"
              quads (statements data-file-path)
              draftset-id (create-draftset! *test-backend* "Test draftset")
              draftset-uri (ontology/draftset-uri draftset-id)]
          (with-open [fs (io/input-stream data-file-path)]
            (let [file-part {:tempfile fs :filename "test-dataset.trig" :content-type "application/x-trig"}
                  request (append-to-draftset-request mount-point draftset-id file-part)
                  {:keys [status body] :as response} (route request)]
              (await-success finished-jobs (:finished-job body))

              (let [draftset-graph-map (get-draftset-graph-mapping *test-backend* draftset-uri)
                    graph-statements (group-by context quads)]
                (doseq [[live-graph graph-quads] graph-statements]
                  (let [draft-graph (get draftset-graph-map live-graph)
                        q (format "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <%s> { ?s ?p ?o } }" draft-graph)
                        draft-statements (repo/query *test-backend* q)
                        expected-statements (map map->Triple graph-quads)]
                    (is (is-graph-managed? *test-backend* live-graph))
                    (is (draft-exists? *test-backend* draft-graph))
                    (is (set expected-statements) (set draft-statements)))))))))

      (testing "Quad data with valid content type for request"
        (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
          (let [draftset-id (create-draftset! *test-backend* "Test draftset")
                file-part {:tempfile fs :filename "test-draftset.trig"}
                request (-> (append-to-draftset-request mount-point draftset-id file-part)
                            (assoc-in [:params :content-type] "application/x-trig"))
                response (route request)]
            (await-success finished-jobs (:finished-job (:body response))))))

      (testing "Triple data"
        (with-open [fs (io/input-stream "test/test-triple.nt")]
          (let [draftset-id (create-draftset! *test-backend* "Test draftset")
                file-part {:tempfile fs :filename "test-triple.nt" :content-type "application/n-triples"}
                request (append-to-draftset-request mount-point draftset-id file-part)
                response (route request)]
            (is (is-client-error-response? response)))))

      (testing "Quad data without content type"
        (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
          (let [draftset-id (create-draftset! *test-backend* "Test draftset")
                file-part {:tempfile fs :filename "test-dataset.trig"}
                request (append-to-draftset-request mount-point draftset-id file-part)
                response (route request)]
            (is (is-client-error-response? response))))))))

(deftest drafts-api-routes-test
  (testing "POST /draft"
    (testing "with a file"
      (let [dest-graph "http://mygraph/graph-to-be-appended-to"
            test-request (-> {:uri "/draft" :request-method :post}
                             (add-request-graph-source-file dest-graph "./test/test-triple.nt"))

            route (draft-api-routes "/draft" *test-backend*)
            {:keys [status body headers] :as response} (route test-request)]

        (job-is-accepted response)
        (is (= {:type :ok} (await-completion finished-jobs (:finished-job body))))

        (testing "appends RDF to the graph"
          (is (repo/query *test-backend* (str "ASK WHERE { GRAPH <" dest-graph "> {<http://example.org/test/triple> ?p ?o . }}"))))))

    (testing "with an invalid RDF file"
      (let [test-request (-> {:uri "/draft" :request-method :post}
                             ;; project.clj is not RDF
                             (add-request-graph-source-file "http://mygraph/graph-to-be-appended-to" "project.clj"))

            route (draft-api-routes "/draft" *test-backend*)
            {:keys [body] :as response} (route test-request)]

        (job-is-accepted response)

        (testing "Stores ParseException in DONE atom of responses for later collection via REST API"
          (let [guid (:finished-job body)
                error-result (await-completion finished-jobs guid)]

            (is (= :error (:type error-result)))
            (is (instance? org.openrdf.rio.RDFParseException (:exception error-result)))))))

    (testing "with a missing content type"
      (let [test-request (-> {:uri "/draft" :request-method :post}
                             (add-request-graph-source-file "http://mygraph/graph-to-be-appended-to" "./test/test-triple.nt")
                             (update-in [:params :file] dissoc :content-type))
            route (draft-api-routes "/draft" *test-backend*)
            {:keys [status] :as response} (route test-request)]
        (is (= 400 status) "Bad request")))

    (testing "with an unknown content type"
      (let [test-request (-> {:uri "/draft" :request-method :post}
                             (add-request-graph-source-file "http://mygraph/graph-to-be-appended-to" "./test/test-triple.nt")
                             (assoc-in [:params :file :content-type] "text/not-a-real-content-type"))
            route (draft-api-routes "/draft" *test-backend*)
            {:keys [status] :as response} (route test-request)]

        (is (= 400 status) "Bad request")))))

(deftest graph-management-delete-graph-test
  (testing "DELETE /graph (batched)"
    (let [graph-uri "http://mygraph/draft-graph5"
          _ (create-managed-graph! *test-backend* graph-uri)
          draft-graph-uri (import-data-to-draft! *test-backend* graph-uri test-triples-4)
          original-batch-size batched-write-size]

      (is (repo/query *test-backend* (str "ASK WHERE { GRAPH <" draft-graph-uri "> { ?s ?p ?o } }"))
          "Graph should exist before deletion")

      (set-var-root! #'batched-write-size 1)

      (let [route (graph-management-routes "/graph" *test-backend*)
            test-request (-> {:uri "/graph" :request-method :delete}
                             (add-request-graph draft-graph-uri))
            response (route test-request)]

        (job-is-accepted response)
        (await-success finished-jobs (:finished-job (:body response)))

        (set-var-root! #'batched-write-size original-batch-size)

        (testing "batched delete job actually deletes the graph"
          (is (not (repo/query *test-backend* (str "ASK WHERE { GRAPH <" draft-graph-uri "> { ?s ?p ?o } }")))
              "Graph should be deleted"))

        (testing "draft removed from state graph"
          (is (not (draft-exists? *test-backend* draft-graph-uri))))))))

(deftest graph-delete-draft-graph-contents-test
  (testing "DELETE /draft/contents (batched)"
    (let [graph-uri "http://mygraph/draft-graph4"
          _ (create-managed-graph! *test-backend* graph-uri)
          draft-graph-uri (import-data-to-draft! *test-backend* graph-uri test-triples-4)]

      (is (repo/query *test-backend* (str "ASK WHERE { GRAPH <" draft-graph-uri "> { ?s ?p ?o } }"))
          "Graph should exist before deletion")

      (let [route (draft-api-routes "/draft" *test-backend*)
            test-request (-> {:uri "/draft/contents" :request-method :delete}
                             (add-request-graph draft-graph-uri))
            response (route test-request)]

        (job-is-accepted response)
        (await-success finished-jobs (:finished-job (:body response)))

        (testing "contents delete job actually deletes the contents but leaves graph intact"
          (is (not (repo/query *test-backend* (str "ASK WHERE { GRAPH <" draft-graph-uri "> { ?s ?p ?o } }")))
              "Graph should be left intact without contents")

          (is (draft-exists? *test-backend* draft-graph-uri)))))))

(deftest graph-management-live-test-with-one-graph
  (testing "PUT /graph/live"
    (let [draft-graph (import-data-to-draft! *test-backend* "http://mygraph.com/live-graph" (test-triples "http://test.com/subject-1"))]
      (is (repo/query *test-backend* (str "ASK WHERE { GRAPH <" draft-graph "> { <http://test.com/subject-1> ?p ?o } }"))
          "Draft graph should exist before deletion")

      (let [route (graph-management-routes "/graph" *test-backend*)
            test-request (-> {:uri "/graph/live" :request-method :put
                              :params {:graph draft-graph}})

            {:keys [body] :as response} (route test-request)]

        (job-is-accepted response)
        (await-success finished-jobs (:finished-job body))

        (testing "moves the draft to live"
          (is (repo/query *test-backend* "ASK WHERE { GRAPH <http://mygraph.com/live-graph> { <http://test.com/subject-1> ?p ?o } }")
              "Live graph should contain our triples"))))))

(deftest graph-management-live-test-multiple-graphs
  (testing "PUT /graph/live"
    (let [draft-graph-1 (import-data-to-draft! *test-backend* "http://mygraph.com/live-graph-1" (test-triples "http://test.com/subject-1"))
          draft-graph-2 (import-data-to-draft! *test-backend* "http://mygraph.com/live-graph-2" (test-triples "http://test.com/subject-1"))]

      (let [route (graph-management-routes "/graph" *test-backend*)
            test-request {:uri "/graph/live"
                          :request-method :put
                          :params {:graph [draft-graph-1 draft-graph-2]}}

            {:keys [status body headers] :as response} (route test-request)]

        (job-is-accepted response)
        (await-success finished-jobs (:finished-job body))

        (testing "moves the draft to live"
          (is (repo/query *test-backend* "ASK WHERE { GRAPH <http://mygraph.com/live-graph-1> { <http://test.com/subject-1> ?p ?o } }")
              "Live graph should contain our triples")
          (is (repo/query *test-backend* "ASK WHERE { GRAPH <http://mygraph.com/live-graph-2> { <http://test.com/subject-1> ?p ?o } }")
              "Live graph should contain our triples"))))))

(defn metadata-exists-sparql [draft-graph-uri name]
  (let [meta-subject (meta-uri name)]
    (str "ASK WHERE { GRAPH <" drafter-state-graph "> { <" draft-graph-uri "> <" meta-subject "> ?v } }")))

(defn metadata-has-value-sparql [draft-graph-uri name value]
  (let [meta-subject (meta-uri name)]
    (str "ASK WHERE { GRAPH <" drafter-state-graph "> { <" draft-graph-uri "> <" meta-subject "> \"" value "\" } }")))

(defn metadata-values-sparql [draft-graph-uri name]
  (let [meta-subject (meta-uri name)]
    (str "SELECT ?o WHERE { GRAPH <" drafter-state-graph "> {
           <" draft-graph-uri "> <" meta-subject "> ?o }"
        "}")))

(defn add-request-metadata-pairs [request meta-pairs]
  (reduce (fn [r [k v]] (add-request-metadata r k v)) request meta-pairs))

(deftest draft-graph-metadata
  (let [draft1 "http://graphs.org/1"
        draft2 "http://graphs.org/2"
        meta-pairs [["foo" "bar"] ["quux" "qaal"]]
        route (draft-api-routes "" *test-backend*)
        create-meta-request (fn [graphs meta-pairs]
                              (-> {:uri "/metadata"
                                   :request-method :post
                                   :params {:graph graphs}}
                                  (add-request-metadata-pairs meta-pairs)))]
    (testing "Adds new metadata"
      (let [test-request (create-meta-request [draft1 draft2] meta-pairs)
            {:keys [status]} (route test-request)]
        (is (= 200 status))

        ;;metadata exists
        (doseq [graph [draft1 draft2]
                [k v] meta-pairs]
          (is (repo/query *test-backend* (metadata-has-value-sparql graph k v))))))

    (testing "Updates existing metdata"
      (let [updated-key (ffirst meta-pairs)
            updated-pair [updated-key "new-value"]

            ;;NOTE: ring only creates a collection if a parameter exists
            ;;multiple times in a query string. This also tests updating
            ;;a single graph
            request (create-meta-request draft1 [updated-pair])
            expected-metadata (assoc meta-pairs 0 updated-pair)
            {:keys [status]} (route request)]

        (is (= 200 status))

        (doseq [[k v] expected-metadata]
          (is (repo/query *test-backend* (metadata-has-value-sparql draft1 k v))))))

    (testing "Deletes metadata"
      (let [delete-request {:uri "/metadata"
                            :request-method :delete
                            :params {:graph [draft1 draft2]
                                     :meta-key ["foo" "quux"]}}
            {:keys [status]} (route delete-request)]
        (is (= 200 status))

        (doseq [g [draft1 draft2]
                m ["foo" "quux"]]
          (is (= false (repo/query *test-backend* (metadata-exists-sparql g m)))))))

    (testing "Invalid if no graphs"
      (let [request (-> {:uri "/metadata" :request-method :post}
                        (add-request-metadata-pairs [["foo" "bar"]]))
            {:keys [status]} (route request)]
        (is (= 400 status))))

    (testing "Invalid if no metadata"
      (let [request {:uri "/metadata"
                     :request-method :post
                     :params {:graph [draft1 draft2]}}
            {:keys [status]} (route request)]
        (is (= 400 status))))))

(defn uriify-values [m]
  (map-values #(and % (URIImpl. %)) m))

(deftest copy-from-live-graph-test
  (let [live-triples (triplify [(URIImpl. "http://subj")
                                [(URIImpl. "http://p1") (URIImpl. "http://o1")]
                                [(URIImpl. "http://p2") (URIImpl. "http://o2")]])
        route (draft-api-routes "/draft" *test-backend*)
        live-graph (create-managed-graph! *test-backend* "http://live")
        draft-graph-uri (create-draft-graph! *test-backend* live-graph)
        copy-request (-> {:uri "/draft/copy-live" :request-method :post}
                           (add-request-graph draft-graph-uri))]

    (add *test-backend* live-graph live-triples)

    (let [{:keys [status body] :as response} (route copy-request)
          job-path (:finished-job body)]
      (is (= 202 status))

      (await-success finished-jobs job-path)

      (let [draft-triples (set (map map->Triple (filter #(= (URIImpl. draft-graph-uri) (:c %)) (statements *test-backend*))))]
        (is (= draft-triples (set live-triples)))))))

(do-template
 [test-name http-method modify-request]

 (deftest test-name
   (testing "Updating draft graph with metadata"
     (testing "Adds metadata"
       (let [route (draft-api-routes "/draft" *test-backend*)
             source-graph-uri (make-graph-live! *test-backend* "http://mygraph/source-graph")
             draft-graph-uri (create-draft-graph! *test-backend* "http://mygraph/dest-graph")

             request (-> {:uri "/draft" :request-method http-method}
                         (add-request-metadata "uploaded-by" "fido")
                         (modify-request draft-graph-uri))

             {:keys [status body]} (route request)]

         (testing "Accepts job"
           (is (= 202 status)))

         (await-success finished-jobs (:finished-job body))

         (let [sparql (metadata-has-value-sparql draft-graph-uri "uploaded-by" "fido")]
           (is (repo/query *test-backend* sparql)))

         (testing "Overwrites metadata"
           ;; try overwriting the value we've just written i.e. replacing
           ;; {uploaded-by fido} with {uploaded-by bonzo}

           (let [new-request (add-request-metadata request "uploaded-by" "bonzo")
                 {:keys [status body]} (route new-request)]

             (testing "Accepts job"
               (is (= 202 status)))

             (await-success finished-jobs (:finished-job body))

             (let [meta-query (metadata-values-sparql draft-graph-uri "uploaded-by")
                   meta-records (repo/query *test-backend* meta-query)]
               (testing "Metadata overwritten"
                 (is (= 1 (count meta-records))
                     (= "updated" (get (first meta-records) "o")))))))))))

 meta-update-with-post-file-test :post (fn [req graph] (add-request-graph-source-file req graph "./test/test-triple.nt")))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
