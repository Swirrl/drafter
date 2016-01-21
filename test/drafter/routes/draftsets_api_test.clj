(ns drafter.routes.draftsets-api-test
  (:require [drafter.test-common :refer [*test-backend* test-triples wrap-clean-test-db wrap-db-setup
                                         stream->string select-all-in-graph make-graph-live!
                                         import-data-to-draft! await-success]]
            [clojure.test :refer :all]
            [clojure.set :as set]
            [drafter.routes.draftsets-api :refer :all]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.rdf.draft-management :refer [is-graph-managed? draft-exists?]]
            [grafter.rdf :refer [statements context add]]
            [grafter.rdf.io :refer [rdf-serializer]]
            [grafter.rdf.formats :as formats]
            [grafter.rdf.protocols :refer [->Triple map->Triple ->Quad]]
            [grafter.rdf.repository :as repo]
            [drafter.util :as util]
            [drafter.responses :refer [is-client-error-response?]]
            [clojure.java.io :as io]
            [schema.core :as s]
            [swirrl-server.async.jobs :refer [finished-jobs]])
  (:import [java.util Date]
           [java.io ByteArrayOutputStream ByteArrayInputStream]))

(defn- append-to-draftset-request [mount-point draftset-location file-part]
  {:uri (str mount-point draftset-location "/data")
   :request-method :post
   :params {:file file-part}})

(defn- create-draftset-request
  ([mount-point display-name] (create-draftset-request mount-point display-name nil))
  ([mount-point display-name description]
   (let [base-params {:display-name display-name}
         params (if (some? description) (assoc base-params :description description) base-params)]
     {:uri (str mount-point "/draftset") :request-method :post :params params})))

(defn- make-append-data-to-draftset-request [route draftset-endpoint-uri data-file-path]
  (with-open [fs (io/input-stream data-file-path)]
    (let [file-part {:tempfile fs :filename "test-dataset.trig" :content-type "application/x-trig"}
          request {:uri (str draftset-endpoint-uri "/data") :request-method :post :params {:file file-part}}]
      (route request))))

(defn- append-data-to-draftset-through-api [route draftset-location draftset-data-file]
  (let [append-response (make-append-data-to-draftset-request route draftset-location draftset-data-file)]
    (await-success finished-jobs (:finished-job (:body append-response)))))

(def ring-response-schema
  {:status s/Int
   :headers {s/Str s/Str}
   :body s/Any})

(def see-other-response-schema
  (merge ring-response-schema
         {:status (s/eq 303)
          :headers {(s/required-key "Location") s/Str}}))

(defn- response-code-schema [code]
  (assoc ring-response-schema :status (s/eq code)))

(def ^:private draftset-without-description-info-schema
  {:id s/Str
   :data {s/Str {s/Any s/Any}}
   :display-name s/Str
   :created-at Date})

(def ^:private draftset-with-description-info-schema
  (assoc draftset-without-description-info-schema :description s/Str))

(def ^:private draftset-info-schema
  (assoc draftset-without-description-info-schema (s/optional-key :description) s/Str))

(defn- assert-schema [schema value]
  (if-let [errors (s/check schema value)]
    (is false errors)))

(defn- assert-is-see-other-response [response]
  (assert-schema see-other-response-schema response))

(defn- assert-is-ok-response [response]
  (assert-schema (response-code-schema 200) response))

(defn- assert-is-not-found-response [response]
  (assert-schema (response-code-schema 404) response))

(defn- assert-is-not-acceptable-response [response]
  (assert-schema (response-code-schema 406) response))

(defn- assert-is-unprocessable-response [response]
  (assert-schema (response-code-schema 422) response))

(defn- assert-is-unsupported-media-type-response [response]
  (assert-schema (response-code-schema 415) response))

(defn- eval-statement [s]
  (util/map-values str s))

(defn- eval-statements [ss]
  (map eval-statement ss))

(defn- concrete-statements [source format]
  (eval-statements (statements source :format format)))

(defn- create-draftset-through-api [mount-point route display-name]
  (let [request (create-draftset-request mount-point display-name)
        {:keys [headers] :as response} (route request)]
    (assert-is-see-other-response response)
    (get headers "Location")))

(defn- create-routes []
  {:mount-point "" :route (draftset-api-routes "" *test-backend*)})

(deftest create-draftset-test
  (let [{:keys [mount-point route]} (create-routes)]
    (testing "Create draftset with title"
      (let [response (route (create-draftset-request mount-point "Test Title!"))]
        (assert-is-see-other-response response)))

    (testing "Create draftset without title"
      (let [response (route {:uri (str mount-point "/draftset") :request-method :post})]
        (assert-is-not-acceptable-response response)))

    (testing "Get non-existent draftset"
      (let [response (route {:uri (str mount-point "/draftset/missing") :request-method :get})]
        (assert-is-not-found-response response)))))

(deftest get-all-draftsets-test
  (let [{:keys [mount-point route]} (create-routes)]
    (let [draftset-count 10
          titles (map #(str "Title" %) (range 1 (inc draftset-count)))
          create-requests (map #(create-draftset-request mount-point %) titles)
          create-responses (doall (map route create-requests))]
        (doseq [r create-responses]
          (assert-is-see-other-response r))

        (let [get-all-request {:uri (str mount-point "/draftsets") :request-method :get}
              {:keys [body] :as response} (route get-all-request)]
          (assert-is-ok-response response)
          (is (= draftset-count (count body)))
          (assert-schema [draftset-without-description-info-schema] body)))))

(deftest get-draftset-test
  (let [{:keys [mount-point route]} (create-routes)]
    (testing "Get empty draftset without description"
      (let [display-name "Test title!"
            create-request (create-draftset-request mount-point display-name)
            create-response (route create-request)]
        (assert-is-see-other-response create-response)

        (let [draftset-location (get-in create-response [:headers "Location"])
              get-request {:uri draftset-location :request-method :get}
              {:keys [body] :as response} (route get-request)]
          (assert-is-ok-response response)
          (assert-schema draftset-without-description-info-schema body)
          (is (= display-name (:display-name body))))))

    (testing "Get empty draftset with description"
      (let [display-name "Test title!"
            description "Draftset used in a test"
            create-request (create-draftset-request mount-point display-name description)
            create-response (route create-request)]
        (assert-is-see-other-response create-response)

        (let [draftset-location (get-in create-response [:headers "Location"])
              get-request {:uri draftset-location :request-method :get}
              {:keys [body] :as response} (route get-request)]
          (assert-is-ok-response response)
          (assert-schema draftset-with-description-info-schema body)
          (is (= display-name (:display-name body)))
          (is (= description (:description body))))))

    (testing "Get draftset containing data"
      (let [display-name "Test title!"
            create-request (create-draftset-request mount-point display-name)
            {create-status :status {draftset-location "Location"} :headers :as create-response} (route create-request)
            quads (statements "test/resources/test-draftset.trig")
            live-graphs (set (keys (group-by context quads)))]
        (assert-is-see-other-response create-response)
        (let [append-response (make-append-data-to-draftset-request route draftset-location "test/resources/test-draftset.trig")]
          (await-success finished-jobs (get-in append-response [:body :finished-job]))
          (let [get-request {:uri draftset-location :request-method :get}
                {:keys [body] :as response} (route get-request)]
            (assert-is-ok-response response)
            (assert-schema draftset-without-description-info-schema body)
            
            (is (= display-name (:display-name body)))
            (is (= live-graphs (set (keys (:data body)))))))))))

(deftest append-data-to-draftset-test
  (let [{:keys [mount-point route]} (create-routes)]
    (testing "Quad data with valid content type for file part"
        (let [data-file-path "test/resources/test-draftset.trig"
              quads (statements data-file-path)
              create-request (create-draftset-request mount-point "Test draftset")
              create-response (route create-request)
              draftset-location (create-draftset-through-api mount-point route "Test draftset")
              draftset-id (.substring draftset-location (inc (.lastIndexOf draftset-location "/")))]
          (with-open [fs (io/input-stream data-file-path)]
              (let [file-part {:tempfile fs :filename "test-dataset.trig" :content-type "application/x-trig"}
                    request (append-to-draftset-request mount-point draftset-location file-part)
                    {:keys [status body] :as response} (route request)]
                (await-success finished-jobs (:finished-job body))

                (let [draftset-graph-map (dsmgmt/get-draftset-graph-mapping *test-backend* (dsmgmt/->DraftsetId draftset-id))
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
          (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
                file-part {:tempfile fs :filename "test-draftset.trig"}
                request (-> (append-to-draftset-request mount-point draftset-location file-part)
                            (assoc-in [:params :content-type] "application/x-trig"))
                response (route request)]
            (await-success finished-jobs (:finished-job (:body response))))))

      (testing "Triple data"
        (with-open [fs (io/input-stream "test/test-triple.nt")]
          (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
                file-part {:tempfile fs :filename "test-triple.nt" :content-type "application/n-triples"}
                request (append-to-draftset-request mount-point draftset-location file-part)
                response (route request)]
            (is (is-client-error-response? response)))))

      (testing "Quad data without content type"
        (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
          (let [draftset-location (create-draftset-through-api mount-point route "Test draftset!")
                file-part {:tempfile fs :filename "test-dataset.trig"}
                request (append-to-draftset-request mount-point draftset-location file-part)
                response (route request)]
            (is (is-client-error-response? response)))))

      (testing "Invalid draftset"
        (let [append-response (make-append-data-to-draftset-request route "/draftset/missing" "test/resources/test-draftset.trig")]
          (assert-is-not-found-response append-response)))))

(defn- statements->input-stream [statements format]
  (let [bos (ByteArrayOutputStream.)
        serialiser (rdf-serializer bos :format format)]
    (add serialiser statements)
    (ByteArrayInputStream. (.toByteArray bos))))

(defn- get-draftset-quads-through-api [route draftset-location]
  (let [data-request {:uri (str draftset-location "/data") :request-method :get :headers {"Accept" "text/x-nquads"}}
        data-response (route data-request)]
    (assert-is-ok-response data-response)
    (concrete-statements (:body data-response) formats/rdf-nquads)))

(defn- create-delete-quads-request [draftset-location input-stream format]
  (let [file-part {:tempfile input-stream :filename "to-delete.nq" :content-type format}]
    {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}}))

(defn- create-delete-triples-request [draftset-location input-stream format graph]
  (assoc-in (create-delete-quads-request draftset-location input-stream format) [:params :graph] graph))

(deftest delete-draftset-data-test
  (let [{:keys [mount-point route]} (create-routes)
        rdf-data-file "test/resources/test-draftset.trig"]
    (testing "Delete quads"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")]
        (append-data-to-draftset-through-api route draftset-location rdf-data-file)
        (let [draftset-quads (set (statements rdf-data-file))

              ;;NOTE: input data should contain at least two statements in each graph!
              ;;delete one quad from each, so all graphs will be non-empty after delete operation
              to-delete (map (fn [[_ graph-quads]] (first graph-quads)) (group-by context draftset-quads))
              to-delete (conj to-delete (->Quad "http://test-subject" "http://test-predicate" "http://test-obj" "http://missing-graph"))
              input-stream (statements->input-stream to-delete formats/rdf-nquads)
              file-part {:tempfile input-stream :filename "to-delete.nq" :content-type "text/x-nquads"}
              delete-request {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}}
              delete-response (route delete-request)]
            
          (assert-is-ok-response delete-response)
          (assert-schema draftset-info-schema (:body delete-response))

          (let [ds-data-request {:uri (str draftset-location "/data") :request-method :get :headers {"Accept" "text/x-nquads"}}
                ds-data-response (route ds-data-request)
                expected-quads (set/difference draftset-quads to-delete)
                actual-quads (set (eval-statements (get-draftset-quads-through-api route draftset-location)))]
            (is (= (set (eval-statements expected-quads)) actual-quads))))))

    (testing "Delete all quads from graph"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            initial-statements (statements rdf-data-file)
            grouped-statements (group-by context initial-statements)
            [graph graph-statements] (first grouped-statements)]
        (append-data-to-draftset-through-api route draftset-location rdf-data-file)

        (with-open [input-stream (statements->input-stream graph-statements formats/rdf-nquads)]
          (let [delete-request (create-delete-quads-request draftset-location input-stream "text/x-nquads")
                {:keys [body] :as delete-response} (route delete-request)]
            (assert-is-ok-response delete-response)
            (assert-schema draftset-info-schema body)

            (let [remaining-graphs (keys (:data body))
                  expected-graphs (rest (keys grouped-statements))]
              (is (= (set expected-graphs) (set remaining-graphs))))))))

    (testing "Delete triples"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            draftset-quads (set (statements rdf-data-file))]
        (append-data-to-draftset-through-api route draftset-location rdf-data-file)

        (let [[graph graph-quads] (first (group-by context draftset-quads))
              quads-to-delete (take 2 graph-quads)
              triples-to-delete (map map->Triple quads-to-delete)
              input-stream (statements->input-stream triples-to-delete formats/rdf-ntriples)
              file-part {:tempfile input-stream :filename "to-delete.nt" :content-type "application/n-triples"}
              delete-request {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part :graph graph}}
              delete-response (route delete-request)]

          (assert-is-ok-response delete-response)
          (assert-schema draftset-info-schema (:body delete-response))

          (let [quads-after-delete (set (eval-statements (get-draftset-quads-through-api route draftset-location)))
                expected-quads (set (eval-statements (set/difference draftset-quads quads-to-delete)))]

            (is (= expected-quads quads-after-delete))))))

    (testing "Delete all triples from graph"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            draftset-quads (set (statements rdf-data-file))]
        (append-data-to-draftset-through-api route draftset-location rdf-data-file)

        (let [grouped-quads (group-by context draftset-quads)
              [graph graph-quads] (first grouped-quads)
              input-stream (statements->input-stream graph-quads formats/rdf-ntriples)
              delete-request (create-delete-triples-request draftset-location input-stream "application/n-triples" graph)
              {:keys [body] :as delete-response} (route delete-request)]

          (assert-is-ok-response delete-response)
          (assert-schema draftset-info-schema body)

          (let [draftset-graphs (keys (:data body))
                expected-graphs (keys (rest grouped-quads))]
            (is (= (set expected-graphs) (set draftset-graphs)))))))

    (testing "Delete triples without graph"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            draftset-quads (statements rdf-data-file)]
        (append-data-to-draftset-through-api route draftset-location rdf-data-file)

        (with-open [input-stream (statements->input-stream (take 2 draftset-quads) formats/rdf-ntriples)]
          (let [file-part {:tempfile input-stream :filename "to-delete.nt" :content-type "application/n-triples"}
                delete-request {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}}
                delete-response (route delete-request)]
            (assert-is-not-acceptable-response delete-response)))))

    (testing "Missing draftset"
      (with-open [fs (io/input-stream rdf-data-file)]
        (let [file-part {:tempfile fs :filename "to-delete.trig" :content-type "application/x-trig"}
              delete-request {:uri "/draftset/missing/data" :request-method :delete :params {:file file-part}}
              delete-response (route delete-request)]
          (assert-is-not-found-response delete-response))))

    (testing "Invalid serialisation"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            input-stream (ByteArrayInputStream. (.getBytes "not nquads!" "UTF-8"))
            file-part {:tempfile input-stream :filename "to-delete.nq" :content-type "text/x-nquads"}
            delete-request {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}}
            delete-response (route delete-request)]
        (assert-is-unprocessable-response delete-response)))

    (testing "Unknown content type"
      (with-open [input-stream (io/input-stream rdf-data-file)]
        (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
              delete-request (create-delete-quads-request draftset-location input-stream "application/unknown-quads-format")
              delete-response (route delete-request)]
          (assert-is-unsupported-media-type-response delete-response))))))

(deftest delete-draftset-graph-test
  (let [{:keys [mount-point route]} (create-routes)]
    (testing "Deletes graph"
      (let [rdf-data-file "test/resources/test-draftset.trig"
            draftset-location (create-draftset-through-api mount-point route "Test draftset")
            draftset-quads (statements rdf-data-file)
            grouped-quads (group-by context draftset-quads)
            [graph _] (first grouped-quads)]
        (append-data-to-draftset-through-api route draftset-location rdf-data-file)

        (let [delete-graph-request {:uri (str draftset-location "/graph") :request-method :delete :params {:graph graph}}
              delete-response (route delete-graph-request)]
          (assert-is-ok-response delete-response)

          (let [remaining-quads (eval-statements (get-draftset-quads-through-api route draftset-location))
                expected-quads (eval-statements (mapcat second (rest grouped-quads)))]
            (is (= (set expected-quads) (set remaining-quads))))

          (let [get-request {:uri draftset-location :request-method :get}
                get-response (route get-request)
                draftset-info (:body get-response)
                draftset-graphs (keys (:data draftset-info))
                expected-graphs (map first (rest grouped-quads))]
            (is (= (set expected-graphs) (set draftset-graphs)))))))

    (testing "Unknown draftset"
      (let [delete-graph-request {:uri "/draftset/missing/graph" :request-method :delete :params {:graph "http://some-graph"}}
            response (route delete-graph-request)]
        (assert-is-not-found-response response)))))

(deftest publish-draftset-test
  (let [{:keys [mount-point route]} (create-routes)
        rdf-data-file "test/resources/test-draftset.trig"]
    (testing "Publish existing draftset"
      (with-open [fs (io/input-stream rdf-data-file)]
        (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
              file-part {:tempfile fs :filename "test-draftset.trig" :content-type "application/x-trig"}
              append-request (append-to-draftset-request mount-point draftset-location file-part)
              append-response (route append-request)]
          (await-success finished-jobs (:finished-job (:body append-response)))

          (let [publish-request {:uri (str draftset-location "/publish") :request-method :post}
                publish-response (route publish-request)]
            (await-success finished-jobs (:finished-job (:body publish-response)))

            (let [live-statements (repo/query *test-backend* "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
                  expected-statements (statements rdf-data-file)
                  graph-statements (group-by context expected-statements)]
              (doseq [[live-graph contents] graph-statements]
                (let [q (format "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <%s> { ?s ?p ?o } }" live-graph)
                      result-triples (set (map #(util/map-values str %) (repo/query *test-backend* q)))
                      graph-triples (set (map (comp #(util/map-values str %) map->Triple) contents))]
                  (is (= graph-triples result-triples))))

              ;;draftset should no longer exist
              (let [get-response (route {:uri draftset-location :request-method :get})]
                (assert-is-not-found-response get-response)))))))

    (testing "Publish non-existent draftset"
      (let [response (route {:uri (str mount-point "/draftset/missing/publish") :request-method :post})]
        (assert-is-not-found-response response)))))

(deftest delete-draftset-test
  (let [{:keys [mount-point route]} (create-routes)
        rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api mount-point route "Test draftset")
        delete-response (route {:uri draftset-location :request-method :delete})]
    (assert-is-ok-response delete-response)
    
    (let [get-response (route {:uri draftset-location :request-method :get})]
      (assert-is-not-found-response get-response))))

(deftest query-draftset-test
  (let [{:keys [mount-point route]} (create-routes)]
    (testing "Draftset with data"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            draftset-data-file "test/resources/test-draftset.trig"
            append-response (make-append-data-to-draftset-request route draftset-location draftset-data-file)]
        (await-success finished-jobs (:finished-job (:body append-response)) )
        (let [query "CONSTRUCT { ?s ?p ?o }  WHERE { GRAPH ?g { ?s ?p ?o } }"
              query-request {:uri (str draftset-location "/query")
                             :headers {"Accept" "application/n-triples"}
                             :request-method :post
                             :params {:query query}}
              query-response (route query-request)
              response-triples (set (map #(util/map-values str %) (statements (:body query-response) :format grafter.rdf.formats/rdf-ntriples)) )
              expected-triples (set (map (comp #(util/map-values str %) map->Triple) (statements draftset-data-file)))]
          (assert-is-ok-response query-response)

          (is (= expected-triples response-triples)))))
    
    (testing "Missing draftset"
      (let [response (route {:uri "/draftset/missing/query" :params {:query "SELECT * WHERE { ?s ?p ?o }"} :request-method :post})]
        (assert-is-not-found-response response)))

    (testing "Missing query parameter"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            response (route {:uri (str draftset-location "/query") :request-method :post})]
        (assert-is-not-acceptable-response response)))

    (testing "Invalid HTTP method"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            query-request {:uri (str draftset-location "/query")
                           :request-method :put
                           :headers {"Accept" "text/plain"}
                           :params {:query "SELECT * WHERE { ?s ?p ?o }"}}
            response (route query-request)]
        (assert-is-not-found-response response)))))

(deftest get-draftset-data-test
  (let [{:keys [mount-point route]} (create-routes)]
    (testing "Get graph triples from draftset"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            draftset-data-file "test/resources/test-draftset.trig"
            input-quads (statements draftset-data-file)
            append-response (make-append-data-to-draftset-request route draftset-location draftset-data-file)]
        (await-success finished-jobs (:finished-job (:body append-response)))

        (doseq [[graph quads] (group-by context input-quads)]
          (let [data-request {:uri (str draftset-location "/data")
                              :request-method :get
                              :params {:graph graph}
                              :headers {"Accept" "application/n-triples"}}
                data-response (route data-request)
                graph-triples (set (map (comp #(util/map-values str %) map->Triple) quads))
                response-triples (statements (:body data-response) :format grafter.rdf.formats/rdf-ntriples)
                response-triples (set (map (comp #(util/map-values str %) map->Triple) response-triples))]
            (assert-is-ok-response data-response)
            (is (= graph-triples response-triples))))))

    (testing "Get all draftset quads"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            draftset-data-file "test/resources/test-draftset.trig"]
        (append-data-to-draftset-through-api route draftset-location draftset-data-file)
        (let [data-request {:uri (str draftset-location "/data") :request-method :get :headers {"Accept" "text/x-nquads"}}
              data-response (route data-request)]
          (assert-is-ok-response data-response)

          (let [response-quads (set (eval-statements (statements (:body data-response) :format grafter.rdf.formats/rdf-nquads)))
                input-quads (set (eval-statements (statements draftset-data-file)))]
            (is (= input-quads response-quads))))))

    (testing "Triples request without graph"
      (let [draftset-location (create-draftset-through-api mount-point route "Test draftset")
            append-response (make-append-data-to-draftset-request route draftset-location "test/resources/test-draftset.trig")]
        (await-success finished-jobs (:finished-job (:body append-response)))
        (let [data-request {:uri (str draftset-location "/data")
                            :request-method :get
                            :headers {"Accept" "application/n-triples"}}
              data-response (route data-request)]
          (assert-is-not-acceptable-response data-response))))

    (testing "Missing draftset"
      (let [response (route {:uri "/draftset/missing/data" :request-method :get :headers {"Accept" "application/n-quads"}})]
        (assert-is-not-found-response response)))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
