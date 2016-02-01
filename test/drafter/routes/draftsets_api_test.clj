(ns drafter.routes.draftsets-api-test
  (:require [drafter.test-common :refer [*test-backend* test-triples wrap-clean-test-db wrap-db-setup
                                         stream->string select-all-in-graph make-graph-live!
                                         import-data-to-draft! await-success key-set]]
            [clojure.test :refer :all]
            [clojure.set :as set]
            [drafter.routes.draftsets-api :refer :all]
            [drafter.rdf.draftset-management :as dsmgmt]
            [drafter.rdf.draft-management :refer [is-graph-managed? draft-exists?]]
            [grafter.rdf :refer [statements context add]]
            [grafter.rdf.io :refer [rdf-serializer]]
            [grafter.rdf.formats :as formats]
            [grafter.rdf.protocols :refer [->Triple map->Triple ->Quad map->Quad]]
            [grafter.rdf.repository :as repo]
            [drafter.util :as util]
            [drafter.responses :refer [is-client-error-response?]]
            [clojure.java.io :as io]
            [schema.core :as s]
            [swirrl-server.async.jobs :refer [finished-jobs]])
  (:import [java.util Date]
           [java.io ByteArrayOutputStream ByteArrayInputStream BufferedReader]
           [org.openrdf.query TupleQueryResultHandler]
           [org.openrdf.query.resultio.sparqljson SPARQLResultsJSONParser]
           [org.openrdf.query.resultio.text.csv SPARQLResultsCSVParser]))

(def ^:private ^:dynamic *route*)

(defn- route [request]
  (*route* request))

(defn- statements->input-stream [statements format]
  (let [bos (ByteArrayOutputStream.)
        serialiser (rdf-serializer bos :format format)]
    (add serialiser statements)
    (ByteArrayInputStream. (.toByteArray bos))))

(defn- append-to-draftset-request [draftset-location file-part]
  {:uri (str draftset-location "/data")
   :request-method :post
   :params {:file file-part}})

(defn- create-draftset-request
  ([display-name] (create-draftset-request display-name nil))
  ([display-name description]
   (let [base-params {:display-name display-name}
         params (if (some? description) (assoc base-params :description description) base-params)]
     {:uri "/draftset" :request-method :post :params params})))

(defn- make-append-data-to-draftset-request [route draftset-endpoint-uri data-file-path]
  (with-open [fs (io/input-stream data-file-path)]
    (let [file-part {:tempfile fs :filename "test-dataset.trig" :content-type "application/x-trig"}
          request {:uri (str draftset-endpoint-uri "/data") :request-method :post :params {:file file-part}}]
      (route request))))

(defn- append-data-to-draftset-through-api [route draftset-location draftset-data-file]
  (let [append-response (make-append-data-to-draftset-request route draftset-location draftset-data-file)]
    (await-success finished-jobs (:finished-job (:body append-response)))))

(defn- statements->append-request [draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)
        file-part {:tempfile input-stream :filename (str "data." (.getDefaultFileExtension format)) :content-type (.getDefaultMIMEType format)}]
    {:uri (str draftset-location "/data") :request-method :post :params {:file file-part}}))

(defn- append-quads-to-draftset-through-api [route draftset-location quads]
  (let [request (statements->append-request draftset-location quads formats/rdf-nquads)
        response (route request)]
    (await-success finished-jobs (get-in response [:body :finished-job]))))

(defn- append-triples-to-draftset-through-api [route draftset-location triples graph]
  (let [request (statements->append-request draftset-location triples formats/rdf-ntriples)
        request (assoc-in request [:params :graph] graph)
        response (route request)]
    (await-success finished-jobs (get-in response [:body :finished-job]))))

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

(def ^:private draftset-without-title-or-description-info-schema
  {:id s/Str
   :data {s/Str {s/Any s/Any}}
   :created-at Date})

(def ^:private draftset-without-description-info-schema
  (assoc draftset-without-title-or-description-info-schema :display-name s/Str))

(def ^:private draftset-with-description-info-schema
  (assoc draftset-without-description-info-schema :description s/Str))

(def ^:private draftset-info-schema
  (merge draftset-without-title-or-description-info-schema
         {(s/optional-key :description) s/Str
          (s/optional-key :display-name) s/Str}))

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

(defn- assert-is-method-not-allowed-response [response]
  (assert-schema (response-code-schema 405) response))

(defn- eval-statement [s]
  (util/map-values str s))

(defn- eval-statements [ss]
  (map eval-statement ss))

(defn- concrete-statements [source format]
  (eval-statements (statements source :format format)))

(defn- create-draftset-through-api [route display-name]
  (let [request (create-draftset-request display-name)
        {:keys [headers] :as response} (route request)]
    (assert-is-see-other-response response)
    (get headers "Location")))

(defn- get-draftset-quads-through-api
  ([route draftset-location]
   (get-draftset-quads-through-api route draftset-location false))
  ([route draftset-location union-with-live?]
   (let [data-request {:uri (str draftset-location "/data")
                       :request-method :get
                       :headers {"Accept" "text/x-nquads"}
                       :params {:union-with-live union-with-live?}}
         data-response (route data-request)]
     (assert-is-ok-response data-response)
     (concrete-statements (:body data-response) formats/rdf-nquads))))

(defn- get-draftset-graph-triples-through-api [route draftset-location graph union-with-live?]
  (let [data-request {:uri (str draftset-location "/data")
                      :request-method :get
                      :headers {"Accept" "application/n-triples"}
                      :params {:union-with-live union-with-live? :graph graph}}
        {:keys [body] :as data-response} (route data-request)]
    (assert-is-ok-response data-response)
    (concrete-statements body formats/rdf-ntriples)))

(defn- publish-draftset-through-api [route draftset-location]
  (let [publish-request {:uri (str draftset-location "/publish") :request-method :post}
        publish-response (route publish-request)]
    (await-success finished-jobs (:finished-job (:body publish-response)))))

(defn- publish-quads-through-api [route quads]
  (let [draftset-location (create-draftset-through-api route "tmp")]
    (append-quads-to-draftset-through-api route draftset-location quads)
    (publish-draftset-through-api route draftset-location)))

(defn- create-delete-quads-request [draftset-location input-stream format]
  (let [file-part {:tempfile input-stream :filename "to-delete.nq" :content-type format}]
    {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}}))

(defn- get-draftset-info-through-api [route draftset-location]
  (let [request {:uri draftset-location :request-method :get}
        {:keys [body] :as response} (route request)]
    (assert-is-ok-response response)
    (assert-schema draftset-info-schema body)
    body))

(defn- delete-draftset-graph-through-api [route draftset-location graph-to-delete]
  (let [delete-graph-request {:uri (str draftset-location "/graph") :request-method :delete :params {:graph graph-to-delete}}
        delete-graph-response (route delete-graph-request)]
    (assert-is-ok-response delete-graph-response)))

(defn- create-delete-triples-request [draftset-location input-stream format graph]
  (assoc-in (create-delete-quads-request draftset-location input-stream format) [:params :graph] graph))

(defn- await-delete-statements-response [response]
  (let [job-result (await-success finished-jobs (get-in response [:body :finished-job]))]
    (:draftset job-result)))

(defn- create-delete-statements-request [draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)
        file-name (str "to-delete." (.getDefaultFileExtension format))
        file-part {:tempfile input-stream :filename file-name  :content-type (.getDefaultMIMEType format)}]
    {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}}))

(defn- delete-quads-through-api [route draftset-location quads]
  (let [delete-request (create-delete-statements-request draftset-location quads formats/rdf-nquads)
        delete-response (route delete-request)]
    (await-delete-statements-response delete-response)))

(defn- delete-draftset-triples-through-api [route draftset-location triples graph]
  (let [delete-request (create-delete-statements-request draftset-location triples formats/rdf-ntriples)
        delete-request (assoc-in delete-request [:params :graph] graph)
        delete-response (route delete-request)]
    (await-delete-statements-response delete-response)))

(deftest create-draftset-test
  (testing "Create draftset with title"
      (let [response (route (create-draftset-request "Test Title!"))]
        (assert-is-see-other-response response)))

    (testing "Create draftset without title"
      (let [response (route {:uri "/draftset" :request-method :post})]
        (assert-is-see-other-response response)))

    (testing "Get non-existent draftset"
      (let [response (route {:uri  "/draftset/missing" :request-method :get})]
        (assert-is-not-found-response response))))

(deftest get-all-draftsets-test
  (let [draftset-count 10
        titles (map #(str "Title" %) (range 1 (inc draftset-count)))
        create-requests (map create-draftset-request titles)
        create-responses (doall (map route create-requests))]
    (doseq [r create-responses]
      (assert-is-see-other-response r))

    (let [get-all-request {:uri "/draftsets" :request-method :get}
          {:keys [body] :as response} (route get-all-request)]
      (assert-is-ok-response response)
      
      (is (= draftset-count (count body)))
      (assert-schema [draftset-without-description-info-schema] body))))

(deftest get-draftset-test
  (testing "Get empty draftset without title or description"
    (let [create-request {:uri "/draftset" :request-method :post}
          create-response (route create-request)]
      (assert-is-see-other-response create-response)

      (let [draftset-location (get-in create-response [:headers "Location"])
            get-request {:uri draftset-location :request-method :get}
            {:keys [body] :as get-response} (route get-request)]
        
        (assert-is-ok-response get-response)
        (assert-schema draftset-without-title-or-description-info-schema body))))
    
  (testing "Get empty draftset without description"
    (let [display-name "Test title!"
          create-request (create-draftset-request display-name)
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
          create-request (create-draftset-request display-name description)
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
          create-request (create-draftset-request display-name)
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
          (is (= live-graphs (set (keys (:data body))))))))))

(deftest append-data-to-draftset-test
  (testing "Quad data with valid content type for file part"
    (let [data-file-path "test/resources/test-draftset.trig"
          quads (statements data-file-path)
          create-request (create-draftset-request "Test draftset")
          create-response (route create-request)
          draftset-location (create-draftset-through-api route "Test draftset")
          draftset-id (.substring draftset-location (inc (.lastIndexOf draftset-location "/")))]
      (with-open [fs (io/input-stream data-file-path)]
        (let [file-part {:tempfile fs :filename "test-dataset.trig" :content-type "application/x-trig"}
              request (append-to-draftset-request draftset-location file-part)
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

  (testing "Appending quads to graph which exist in live"
    (let [quads (statements "test/resources/test-draftset.trig")
          grouped-quads (group-by context quads)
          live-quads (map (comp first second) grouped-quads)
          quads-to-add (rest (second (first grouped-quads)))
          draftset-location (create-draftset-through-api route "Test draftset")]
      (publish-quads-through-api route live-quads)
      (append-quads-to-draftset-through-api route draftset-location quads-to-add)

      ;;draftset itself should contain the live quads from the graph
      ;;added to along with the quads explicitly added. It should
      ;;not contain any quads from the other live graph.
      (let [draftset-quads (get-draftset-quads-through-api route draftset-location false)
            expected-quads (eval-statements (second (first grouped-quads)))]
        (is (= (set expected-quads) (set draftset-quads))))))

  (testing "Quad data with valid content type for request"
    (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
      (let [draftset-location (create-draftset-through-api route "Test draftset")
            file-part {:tempfile fs :filename "test-draftset.trig"}
            request (-> (append-to-draftset-request draftset-location file-part)
                        (assoc-in [:params :content-type] "application/x-trig"))
            response (route request)]
        (await-success finished-jobs (:finished-job (:body response))))))

  (testing "Triple data"
    (with-open [fs (io/input-stream "test/test-triple.nt")]
      (let [draftset-location (create-draftset-through-api route "Test draftset")
            file-part {:tempfile fs :filename "test-triple.nt" :content-type "application/n-triples"}
            request (append-to-draftset-request draftset-location file-part)
            response (route request)]
        (is (is-client-error-response? response)))))

  (testing "Triples for graph which exists in live"
    (let [[graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (create-draftset-through-api route "Test draftset")]
      (publish-quads-through-api route [(first graph-quads)])
      (append-triples-to-draftset-through-api route draftset-location (rest graph-quads) graph)

      (let [draftset-graph-triples (get-draftset-graph-triples-through-api route draftset-location graph false)
            expected-triples (eval-statements (map map->Triple graph-quads))]
        (is (= (set expected-triples) (set draftset-graph-triples))))))

  (testing "Quad data without content type"
    (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
      (let [draftset-location (create-draftset-through-api route "Test draftset!")
            file-part {:tempfile fs :filename "test-dataset.trig"}
            request (append-to-draftset-request draftset-location file-part)
            response (route request)]
        (is (is-client-error-response? response)))))

  (testing "Invalid draftset"
    (let [append-response (make-append-data-to-draftset-request route "/draftset/missing" "test/resources/test-draftset.trig")]
      (assert-is-not-found-response append-response))))

(deftest delete-draftset-data-test
  (let [rdf-data-file "test/resources/test-draftset.trig"]

    (testing "Delete quads from graphs in live"
      (let [quads (statements "test/resources/test-draftset.trig")
            grouped-quads (group-by context quads)
            to-delete (map (comp first second) grouped-quads)
            draftset-location (create-draftset-through-api route "Test draftset")]
        (publish-quads-through-api route quads)

        (let [{graph-info :data :as draftset-info} (delete-quads-through-api route draftset-location to-delete)
              ds-graphs (keys graph-info)
              expected-graphs (map first grouped-quads)
              draftset-quads (get-draftset-quads-through-api route draftset-location false)
              expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))]
          (is (= (set expected-graphs) (set ds-graphs)))
          (is (= (set expected-quads) (set draftset-quads))))))

    (testing "Delete quads from graph not in live"
      (let [draftset-location (create-draftset-through-api route "Test draftset")
            to-delete [(->Quad "http://s1" "http://p1" "http://o1" "http://missing-graph1")
                       (->Quad "http://s2" "http://p2" "http://o2" "http://missing-graph2")]
            draftset-info (delete-quads-through-api route draftset-location to-delete)]
        (is (empty? (keys (:data draftset-info))))))
    
    (testing "Delete quads only in draftset"
      (let [draftset-location (create-draftset-through-api route "Test draftset")
            draftset-quads (statements rdf-data-file)
            grouped-quads (group-by context draftset-quads)]

        (append-quads-to-draftset-through-api route draftset-location draftset-quads)
        
        (let [
              ;;NOTE: input data should contain at least two statements in each graph!
              ;;delete one quad from each, so all graphs will be non-empty after delete operation
              to-delete (map (comp first second) grouped-quads)
              draftset-info (delete-quads-through-api route draftset-location to-delete)
              expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))
              actual-quads (get-draftset-quads-through-api route draftset-location false)]
          (is (= (set expected-quads) (set actual-quads))))))

    (testing "Delete all quads from graph"
      (let [draftset-location (create-draftset-through-api route "Test draftset")
            initial-statements (statements rdf-data-file)
            grouped-statements (group-by context initial-statements)
            [graph graph-statements] (first grouped-statements)]
        (append-data-to-draftset-through-api route draftset-location rdf-data-file)

        (let [draftset-info (delete-quads-through-api route draftset-location graph-statements)
              expected-graphs (set (map :c initial-statements))
              draftset-graphs (key-set (:data draftset-info))]
          ;;graph should still be in draftset even if it is empty since it should be deleted on publish
          (is (= expected-graphs draftset-graphs)))))

    (testing "Delete triples from graph in live"
      (let [quads (statements "test/resources/test-draftset.trig")
            grouped-quads (group-by context quads)
            [live-graph graph-quads] (first grouped-quads)
            draftset-location (create-draftset-through-api route "Test draftset")]

        (publish-quads-through-api route quads)
        (let [draftset-info (delete-quads-through-api route draftset-location [(first graph-quads)])
              draftset-quads (get-draftset-quads-through-api route draftset-location false)
              expected-quads (eval-statements (rest graph-quads))]
          (is (= #{live-graph} (key-set (:data draftset-info))))
          (is (= (set expected-quads) (set draftset-quads))))))

    (testing "Delete triples from graph not in live"
      (let [draftset-location (create-draftset-through-api route "Test draftset")
            to-delete [(->Triple "http://s1" "http://p1" "http://o1")
                       (->Triple "http://s2" "http://p2" "http://o2")]
            draftset-info (delete-draftset-triples-through-api route draftset-location to-delete "http://missing")
            draftset-quads (get-draftset-quads-through-api route draftset-location false)]

        ;;graph should not exist in draftset since it was not in live
        (is (empty? (:data draftset-info)))
        (is (empty? draftset-quads))))
    
    (testing "Delete triples only in draftset"
      (let [draftset-location (create-draftset-through-api route "Test draftset")
            draftset-quads (set (statements rdf-data-file))
            [graph graph-quads] (first (group-by context draftset-quads))
            quads-to-delete (take 2 graph-quads)
            triples-to-delete (map map->Triple quads-to-delete)]
        
        (append-data-to-draftset-through-api route draftset-location rdf-data-file)

        (let [draftset-info (delete-draftset-triples-through-api route draftset-location triples-to-delete graph)
              quads-after-delete (set (get-draftset-quads-through-api route draftset-location))
              expected-quads (set (eval-statements (set/difference draftset-quads quads-to-delete)))]
          (is (= expected-quads quads-after-delete)))))

    (testing "Delete all triples from graph"
      (let [quads (statements rdf-data-file)
            grouped-quads (group-by context quads)
            [graph graph-quads] (first grouped-quads)
            triples-to-delete (map map->Triple graph-quads)
            draftset-location (create-draftset-through-api route "Test draftset")
            draftset-quads (set (statements rdf-data-file))]
        
        (publish-quads-through-api route quads)

        (let [draftset-info (delete-draftset-triples-through-api route draftset-location triples-to-delete graph)
              draftset-quads (get-draftset-quads-through-api route draftset-location false)
              draftset-graphs (key-set (:data draftset-info))]

          (is (= #{graph} draftset-graphs))
          (is (empty? draftset-quads)))))

    (testing "Delete triples without graph"
      (let [draftset-location (create-draftset-through-api route "Test draftset")
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
      (let [draftset-location (create-draftset-through-api route "Test draftset")
            input-stream (ByteArrayInputStream. (.getBytes "not nquads!" "UTF-8"))
            file-part {:tempfile input-stream :filename "to-delete.nq" :content-type "text/x-nquads"}
            delete-request {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}}
            delete-response (route delete-request)]
        (assert-is-unprocessable-response delete-response)))

    (testing "Unknown content type"
      (with-open [input-stream (io/input-stream rdf-data-file)]
        (let [draftset-location (create-draftset-through-api route "Test draftset")
              delete-request (create-delete-quads-request draftset-location input-stream "application/unknown-quads-format")
              delete-response (route delete-request)]
          (assert-is-unsupported-media-type-response delete-response))))))

(deftest delete-draftset-graph-test
  (testing "Delete non-existent live graph"
    (let [draftset-location (create-draftset-through-api route "Test draftset")
          graph-to-delete "http://live-graph"
          delete-graph-request {:uri (str draftset-location "/graph") :request-method :delete :params {:graph graph-to-delete}}
          delete-graph-response (route delete-graph-request)]
      (assert-is-ok-response delete-graph-response)

      (let [draftset-info (get-draftset-info-through-api route draftset-location)]
        ;;graph to delete should NOT exist in the draftset since it did not exist in live
        ;;at the time of the delete
        (is (= #{} (set (keys (:data draftset-info))))))))

  (testing "Delete live graph not in draftset"
    (let [quads (statements "test/resources/test-draftset.trig")
          graph-quads (group-by context quads)
          live-graphs (keys graph-quads)
          graph-to-delete (first live-graphs)
          draftset-location (create-draftset-through-api route "Test draftset")]
      (publish-quads-through-api route quads)
      (delete-draftset-graph-through-api route draftset-location graph-to-delete)

      (let [{draftset-graphs :data} (get-draftset-info-through-api route draftset-location)]
        (is (= #{graph-to-delete} (set (keys draftset-graphs)))))))

  (testing "Delete graph with changes in draftset"
    (let [draftset-location (create-draftset-through-api route "Test draftset")
          [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          published-quad (first graph-quads)
          added-quads (rest graph-quads)]
      (publish-quads-through-api route [published-quad])
      (append-quads-to-draftset-through-api route draftset-location added-quads)
      (delete-draftset-graph-through-api route draftset-location graph)

      (let [{draftset-graphs :data} (get-draftset-info-through-api route draftset-location)]
        (is (= #{graph} (set (keys draftset-graphs)))))))
    
  (testing "Deletes graph only in draftset"
    (let [rdf-data-file "test/resources/test-draftset.trig"
          draftset-location (create-draftset-through-api route "Test draftset")
          draftset-quads (statements rdf-data-file)
          grouped-quads (group-by context draftset-quads)
          [graph _] (first grouped-quads)]
      (append-data-to-draftset-through-api route draftset-location rdf-data-file)

      (delete-draftset-graph-through-api route draftset-location graph)
      
      (let [remaining-quads (eval-statements (get-draftset-quads-through-api route draftset-location))
            expected-quads (eval-statements (mapcat second (rest grouped-quads)))]
        (is (= (set expected-quads) (set remaining-quads))))

      (let [draftset-info (get-draftset-info-through-api route draftset-location)
            draftset-graphs (keys (:data draftset-info))
            expected-graphs (keys grouped-quads)]
        (is (= (set expected-graphs) (set draftset-graphs))))))

  (testing "Unknown draftset"
    (let [delete-graph-request {:uri "/draftset/missing/graph" :request-method :delete :params {:graph "http://some-graph"}}
          response (route delete-graph-request)]
      (assert-is-not-found-response response))))

;;TODO: Get quads through query of live endpoint? This depends on
;;'union with live' working correctly
(defn- get-live-quads-through-api [route]
  (let [tmp-ds (create-draftset-through-api route "tmp")]
    (get-draftset-quads-through-api route tmp-ds true)))

(defn- assert-live-quads [route expected-quads]
  (let [live-quads (get-live-quads-through-api route)]
    (is (= (set (eval-statements expected-quads)) (set live-quads)))))

(deftest publish-draftset-test
  (let [rdf-data-file "test/resources/test-draftset.trig"
        quads (statements rdf-data-file)
        grouped-quads (doall (group-by context quads))]

    (testing "With new graphs not in live"
      (let [draftset-location (create-draftset-through-api route "Test draftset")]
        (append-quads-to-draftset-through-api route draftset-location quads)
        (publish-draftset-through-api route draftset-location)

        (let [live-quads (get-live-quads-through-api route)]
          (is (= (set (eval-statements quads)) (set live-quads))))))

    (testing "With statements added to graphs in live"
      (let [draftset-location (create-draftset-through-api route "Test draftset")
            initial-live-quads (map (comp first second) grouped-quads)
            appended-quads (mapcat (comp rest second) grouped-quads)]
    
        (publish-quads-through-api route initial-live-quads)
        (append-quads-to-draftset-through-api route draftset-location appended-quads)
        (publish-draftset-through-api route draftset-location)

        (let [after-publish-quads (get-live-quads-through-api route)]
          (is (= (set (eval-statements quads)) (set after-publish-quads))))))

    (testing "With statements deleted from drafts in live"
      (publish-quads-through-api route quads)

      (let [draftset-location (create-draftset-through-api route "Test draftset")
            to-delete (map (comp first second) grouped-quads)]
        (delete-quads-through-api route draftset-location to-delete)
        (publish-draftset-through-api route draftset-location)

        (let [after-publish-quads (get-live-quads-through-api route)
              expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))]
          (is (= (set expected-quads) (set after-publish-quads))))))

    (testing "Deleting graph from live"
      (publish-quads-through-api route quads)

      (let [draftset-location (create-draftset-through-api route "Test draftset")
            graph-to-delete (ffirst grouped-quads)
            expected-quads (eval-statements (mapcat second (rest grouped-quads)))]
        (delete-draftset-graph-through-api route draftset-location graph-to-delete)
        (publish-draftset-through-api route draftset-location)
        (assert-live-quads route expected-quads)))

    (testing "Delete and append from live"
      ;;TODO: refactor tests so this is not required
      (grafter.rdf.protocols/update! *test-backend* "DROP ALL")
      
      (let [[live-graph initial-quads] (first grouped-quads)
            draftset-location (create-draftset-through-api route "Test draftset")
            to-add (take 2 (second (second grouped-quads)))
            to-delete (take 1 initial-quads)
            expected-quads (eval-statements (set/difference (set/union (set initial-quads) (set to-add)) (set to-delete)))]

        (publish-quads-through-api route initial-quads)
        (append-quads-to-draftset-through-api route draftset-location to-add)
        (delete-quads-through-api route draftset-location to-delete)
        (publish-draftset-through-api route draftset-location)

        (assert-live-quads route expected-quads)))

    (testing "Deletion from graph not yet in live"
      ;;TODO: refactor tests so this is not required
      (grafter.rdf.protocols/update! *test-backend* "DROP ALL")
      
      (let [[graph graph-quads] (first grouped-quads)
            draftset-location (create-draftset-through-api route "Test draftset")]

        ;;delete quads in draftset before they exist in live
        (delete-quads-through-api route draftset-location graph-quads)

        ;;add to live then publish draftset
        (publish-quads-through-api route graph-quads)
        (publish-draftset-through-api route draftset-location)

        ;;graph should still exist in live
        (assert-live-quads route graph-quads)))

    (testing "Publish non-existent draftset"
      (let [response (route {:uri "/draftset/missing/publish" :request-method :post})]
        (assert-is-not-found-response response)))))

(deftest delete-draftset-test
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api route "Test draftset")
        delete-response (route {:uri draftset-location :request-method :delete})]
    (assert-is-ok-response delete-response)
    
    (let [get-response (route {:uri draftset-location :request-method :get})]
      (assert-is-not-found-response get-response))))

(defn- result-set-handler [result-state]
  (reify TupleQueryResultHandler
    (handleBoolean [this b])
    (handleLinks [this links])
    (startQueryResult [this binding-names])
    (endQueryResult [this])
    (handleSolution [this binding-set]
      (let [binding-pairs (map (fn [b] [(keyword (.getName b)) (.stringValue (.getValue b))]) binding-set)
            binding-map (into {} binding-pairs)]
        (swap! result-state conj binding-map)))))

(deftest query-draftset-test
  (testing "Draftset with data"
    (let [draftset-location (create-draftset-through-api route "Test draftset")
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

  (testing "Union with live"
    (let [test-quads (statements "test/resources/test-draftset.trig")
          grouped-test-quads (group-by context test-quads)
          [live-graph live-quads] (first grouped-test-quads)
          [ds-live-graph draftset-quads] (second grouped-test-quads)
          draftset-location (create-draftset-through-api route "Test draftset")]

      (publish-quads-through-api route live-quads)
      (append-quads-to-draftset-through-api route draftset-location draftset-quads)

      (let [query "SELECT * WHERE { GRAPH ?c { ?s ?p ?o } }"
            query-request {:uri (str draftset-location "/query")
                           :headers {"Accept" "application/sparql-results+json"}
                           :request-method :post
                           :params {:query query :union-with-live true}}
            {:keys [body] :as query-response} (route query-request)
            result-state (atom #{})
            result-handler (result-set-handler result-state)
            parser (doto (SPARQLResultsCSVParser.) (.setTupleQueryResultHandler result-handler))]

        (.parse parser body)

        (let [expected-quads (set (eval-statements test-quads))]
          (is (= expected-quads @result-state))))))
    
  (testing "Missing draftset"
    (let [response (route {:uri "/draftset/missing/query" :params {:query "SELECT * WHERE { ?s ?p ?o }"} :request-method :post})]
      (assert-is-not-found-response response)))

  (testing "Missing query parameter"
    (let [draftset-location (create-draftset-through-api route "Test draftset")
          response (route {:uri (str draftset-location "/query") :request-method :post})]
      (assert-is-not-acceptable-response response)))

  (testing "Invalid HTTP method"
    (let [draftset-location (create-draftset-through-api route "Test draftset")
          query-request {:uri (str draftset-location "/query")
                         :request-method :put
                         :headers {"Accept" "text/plain"}
                         :params {:query "SELECT * WHERE { ?s ?p ?o }"}}
          response (route query-request)]
      (assert-is-method-not-allowed-response response))))

(deftest get-draftset-data-test
  (testing "Get graph triples from draftset"
    (let [draftset-location (create-draftset-through-api route "Test draftset")
          draftset-data-file "test/resources/test-draftset.trig"
          input-quads (statements draftset-data-file)
          append-response (make-append-data-to-draftset-request route draftset-location draftset-data-file)]
      (await-success finished-jobs (:finished-job (:body append-response)))

      (doseq [[graph quads] (group-by context input-quads)]
        (let [graph-triples (set (eval-statements (map map->Triple quads)))
              response-triples (set (get-draftset-graph-triples-through-api route draftset-location graph false))]
          (is (= graph-triples response-triples))))))

  (testing "Get all draftset quads"
    (let [draftset-location (create-draftset-through-api route "Test draftset")
          draftset-data-file "test/resources/test-draftset.trig"]
      (append-data-to-draftset-through-api route draftset-location draftset-data-file)

      (let [response-quads (set (get-draftset-quads-through-api route draftset-location))
            input-quads (set (eval-statements (statements draftset-data-file)))]
        (is (= input-quads response-quads)))))

  (testing "Deleted draftset quads unioned with live"
    (let [quads (statements "test/resources/test-draftset.trig")
          grouped-quads (group-by context quads)
          graph-to-delete (first (keys grouped-quads))
          draftset-location (create-draftset-through-api route "Test draftset")]
      (publish-quads-through-api route quads)
      (delete-draftset-graph-through-api route draftset-location graph-to-delete)

      (let [response-quads (set (get-draftset-quads-through-api route draftset-location true))
            expected-quads (set (eval-statements (mapcat second (rest grouped-quads))))]
        (is (= expected-quads response-quads)))))

  (testing "Added draftset quads unioned with live"
    (let [quads (statements "test/resources/test-draftset.trig")
          grouped-quads (group-by context quads)
          [live-graph live-quads] (first grouped-quads)
          [draftset-graph draftset-quads] (second grouped-quads)
          draftset-location (create-draftset-through-api route "Test draftset")]
      (publish-quads-through-api route live-quads)
      (append-quads-to-draftset-through-api route draftset-location draftset-quads)

      (let [response-quads (set (get-draftset-quads-through-api route draftset-location true))
            expected-quads (set (eval-statements (concat live-quads draftset-quads)))]
        (is (= expected-quads response-quads)))))

  (testing "Triples for deleted graph unioned with live"
    (let [quads (statements "test/resources/test-draftset.trig")
          grouped-quads (group-by context quads)
          graph-to-delete (ffirst grouped-quads)
          draftset-location (create-draftset-through-api route "Test draftset")]
      (publish-quads-through-api route quads)
      (delete-draftset-graph-through-api route draftset-location graph-to-delete)

      (let [draftset-triples (get-draftset-graph-triples-through-api route draftset-location graph-to-delete true)]
        (is (empty? draftset-triples)))))

  (testing "Triples for published graph not in draftset when unioned with live"
    (let [quads (statements "test/resources/test-draftset.trig")
          [graph graph-quads] (first (group-by context quads))
          draftset-location (create-draftset-through-api route "Test draftset")]
      (publish-quads-through-api route graph-quads)

      (let [draftset-graph-triples (get-draftset-graph-triples-through-api route draftset-location graph true)
            expected-triples (eval-statements (map map->Triple graph-quads))]
        (is (= (set expected-triples) (set draftset-graph-triples))))))
    
  (testing "Triples request without graph"
    (let [draftset-location (create-draftset-through-api route "Test draftset")
          append-response (make-append-data-to-draftset-request route draftset-location "test/resources/test-draftset.trig")]
      (await-success finished-jobs (:finished-job (:body append-response)))
      (let [data-request {:uri (str draftset-location "/data")
                          :request-method :get
                          :headers {"Accept" "application/n-triples"}}
            data-response (route data-request)]
        (assert-is-not-acceptable-response data-response))))

  (testing "Missing draftset"
    (let [response (route {:uri "/draftset/missing/data" :request-method :get :headers {"Accept" "application/n-quads"}})]
      (assert-is-not-found-response response))))

(deftest set-draftset-metadata-test
  (testing "Set metadata"
    (let [draftset-location (create-draftset-through-api route "temp")
          new-title "Updated title"
          new-description "Updated description"
          meta-request {:uri (str draftset-location "/meta") :request-method :put :params {:display-name new-title :description new-description}}
          {:keys [body] :as  meta-response} (route meta-request)]
      
      (assert-is-ok-response meta-response)
      (assert-schema draftset-info-schema body)

      (is (= new-title (:display-name body)))
      (is (= new-description (:description body)))))
    
  (testing "Missing draftest"
    (let [meta-request {:uri "/draftset/missing/meta" :request-method :put :params {:display-name "Title!" :description "Description"}}
          meta-response (route meta-request)]
      (assert-is-not-found-response meta-response))))

(defn- setup-route [test-function]
  (binding [*route* (draftset-api-routes *test-backend*)]
    (test-function)))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each setup-route)
(use-fixtures :each (fn [tf]
                      (wrap-clean-test-db #(setup-route tf))))
