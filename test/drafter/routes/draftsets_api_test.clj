(ns drafter.routes.draftsets-api-test
  (:require [drafter.test-common :refer [*test-backend* test-triples wrap-clean-test-db wrap-db-setup
                                         stream->string select-all-in-graph make-graph-live!
                                         import-data-to-draft! await-success]]
            [clojure.test :refer :all]
            [drafter.routes.draftsets-api :refer :all]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.rdf.draft-management :refer [is-graph-managed? draft-exists?]]
            [grafter.rdf :refer [statements context]]
            [grafter.rdf.protocols :refer [->Triple map->Triple]]
            [grafter.rdf.repository :as repo]
            [drafter.responses :refer [is-client-error-response?]]
            [clojure.java.io :as io]
            [schema.core :as s]
            [swirrl-server.async.jobs :refer [finished-jobs]])
  (:import [java.util Date]))

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
    (testing "Appending data to draftset"
      (testing "Quad data with valid content type for file part"
        (let [data-file-path "test/resources/test-draftset.trig"
              quads (statements data-file-path)
              create-request (create-draftset-request mount-point "Test draftset")
              create-response (route create-request)
              draftset-location (create-draftset-through-api mount-point route "Test draftset")
              draftset-id (.substring draftset-location (inc (.lastIndexOf draftset-location "/")))
              draftset-uri (drafter.rdf.drafter-ontology/draftset-uri draftset-id)]
          (with-open [fs (io/input-stream data-file-path)]
              (let [file-part {:tempfile fs :filename "test-dataset.trig" :content-type "application/x-trig"}
                    request (append-to-draftset-request mount-point draftset-location file-part)
                    {:keys [status body] :as response} (route request)]
                (await-success finished-jobs (:finished-job body))

                (let [draftset-graph-map (dsmgmt/get-draftset-graph-mapping *test-backend* draftset-uri)
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
            (is (is-client-error-response? response))))))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
