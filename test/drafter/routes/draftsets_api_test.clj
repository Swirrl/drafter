(ns drafter.routes.draftsets-api-test
  (:require [drafter.test-common :refer [*test-backend* test-triples wrap-clean-test-db wrap-db-setup
                                         stream->string select-all-in-graph make-graph-live!
                                         import-data-to-draft! await-success key-set]]
            [clojure.test :refer :all]
            [clojure.set :as set]
            [drafter.routes.draftsets-api :refer :all]
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
  ([] {:uri "/draftset" :request-method :post :params {}})
  ([display-name] {:uri "/draftset" :request-method :post :params {:display-name display-name}})
  ([display-name description]
   {:uri "/draftset" :request-method :post :params {:display-name display-name :description description}}))

(defn- make-append-data-to-draftset-request [draftset-endpoint-uri data-file-path]
  (with-open [fs (io/input-stream data-file-path)]
    (let [file-part {:tempfile fs :filename "test-dataset.trig" :content-type "application/x-trig"}
          request {:uri (str draftset-endpoint-uri "/data") :request-method :post :params {:file file-part}}]
      (route request))))

(defn- append-data-to-draftset-through-api [draftset-location draftset-data-file]
  (let [append-response (make-append-data-to-draftset-request draftset-location draftset-data-file)]
    (await-success finished-jobs (:finished-job (:body append-response)))))

(defn- statements->append-request [draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)
        file-part {:tempfile input-stream :filename (str "data." (.getDefaultFileExtension format)) :content-type (.getDefaultMIMEType format)}]
    {:uri (str draftset-location "/data") :request-method :post :params {:file file-part}}))

(defn- append-quads-to-draftset-through-api [draftset-location quads]
  (let [request (statements->append-request draftset-location quads formats/rdf-nquads)
        response (route request)]
    (await-success finished-jobs (get-in response [:body :finished-job]))))

(defn- append-triples-to-draftset-through-api [draftset-location triples graph]
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

(defn- create-draftset-through-api
  ([] (create-draftset-through-api nil))
  ([display-name] (create-draftset-through-api display-name nil))
  ([display-name description]
   (let [request (create-draftset-request display-name description)
         {:keys [headers] :as response} (route request)]
     (assert-is-see-other-response response)
     (get headers "Location"))))

(defn- get-draftset-quads-through-api
  ([draftset-location]
   (get-draftset-quads-through-api draftset-location false))
  ([draftset-location union-with-live?]
   (let [data-request {:uri (str draftset-location "/data")
                       :request-method :get
                       :headers {"Accept" "text/x-nquads"}
                       :params {:union-with-live union-with-live?}}
         data-response (route data-request)]
     (assert-is-ok-response data-response)
     (concrete-statements (:body data-response) formats/rdf-nquads))))

(defn- get-draftset-graph-triples-through-api [draftset-location graph union-with-live?]
  (let [data-request {:uri (str draftset-location "/data")
                      :request-method :get
                      :headers {"Accept" "application/n-triples"}
                      :params {:union-with-live union-with-live? :graph graph}}
        {:keys [body] :as data-response} (route data-request)]
    (assert-is-ok-response data-response)
    (concrete-statements body formats/rdf-ntriples)))

(defn- publish-draftset-through-api [draftset-location]
  (let [publish-request {:uri (str draftset-location "/publish") :request-method :post}
        publish-response (route publish-request)]
    (await-success finished-jobs (:finished-job (:body publish-response)))))

(defn- publish-quads-through-api [route quads]
  (let [draftset-location (create-draftset-through-api)]
    (append-quads-to-draftset-through-api draftset-location quads)
    (publish-draftset-through-api draftset-location)))

(defn- create-delete-quads-request [draftset-location input-stream format]
  (let [file-part {:tempfile input-stream :filename "to-delete.nq" :content-type format}]
    {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}}))

(defn- get-draftset-info-through-api [draftset-location]
  (let [request {:uri draftset-location :request-method :get}
        {:keys [body] :as response} (route request)]
    (assert-is-ok-response response)
    (assert-schema draftset-info-schema body)
    body))

(defn- delete-draftset-graph-through-api [draftset-location graph-to-delete]
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

(defn- delete-quads-through-api [draftset-location quads]
  (let [delete-request (create-delete-statements-request draftset-location quads formats/rdf-nquads)
        delete-response (route delete-request)]
    (await-delete-statements-response delete-response)))

(defn- delete-draftset-triples-through-api [draftset-location triples graph]
  (let [delete-request (create-delete-statements-request draftset-location triples formats/rdf-ntriples)
        delete-request (assoc-in delete-request [:params :graph] graph)
        delete-response (route delete-request)]
    (await-delete-statements-response delete-response)))

;;TODO: Get quads through query of live endpoint? This depends on
;;'union with live' working correctly
(defn- get-live-quads-through-api []
  (let [tmp-ds (create-draftset-through-api "tmp")]
    (get-draftset-quads-through-api tmp-ds true)))

(defn- assert-live-quads [expected-quads]
  (let [live-quads (get-live-quads-through-api)]
    (is (= (set (eval-statements expected-quads)) (set live-quads)))))

(deftest create-draftset-without-title-or-description
  (let [response (route {:uri "/draftset" :request-method :post})]
    (assert-is-see-other-response response)))

(deftest create-draftset-with-title-and-without-description
  (let [response (route (create-draftset-request "Test Title!"))]
    (assert-is-see-other-response response)))

(deftest create-draftset-with-title-and-description
  (let [response (route (create-draftset-request "Test title" "Test description"))]
    (assert-is-see-other-response response)))

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

(deftest get-empty-draftset-without-title-or-description
  (let [draftset-location (create-draftset-through-api)
        get-request {:uri draftset-location :request-method :get}
        {:keys [body] :as get-response} (route get-request)]
    (assert-is-ok-response get-response)
    (assert-schema draftset-without-title-or-description-info-schema body)))

(deftest get-empty-draftset-without-description
  (let [display-name "Test title!"
        draftset-location (create-draftset-through-api display-name)
        get-request {:uri draftset-location :request-method :get}
        {:keys [body] :as response} (route get-request)]
    (assert-is-ok-response response)
    (assert-schema draftset-without-description-info-schema body)
    (is (= display-name (:display-name body)))))

(deftest get-empty-draftset-with-description
  (let [display-name "Test title!"
        description "Draftset used in a test"
        draftset-location (create-draftset-through-api display-name description)]
    
    (let [get-request {:uri draftset-location :request-method :get}
          {:keys [body] :as response} (route get-request)]
      (assert-is-ok-response response)
      (assert-schema draftset-with-description-info-schema body)
      (is (= display-name (:display-name body)))
      (is (= description (:description body))))))

(deftest get-draftset-containing-data
  (let [display-name "Test title!"
        draftset-location (create-draftset-through-api display-name)
        quads (statements "test/resources/test-draftset.trig")
        live-graphs (set (keys (group-by context quads)))]
    (append-quads-to-draftset-through-api draftset-location quads)
    
    (let [get-request {:uri draftset-location :request-method :get}
          {:keys [body] :as response} (route get-request)]
      (assert-is-ok-response response)
      (assert-schema draftset-without-description-info-schema body)
      
      (is (= display-name (:display-name body)))
      (is (= live-graphs (set (keys (:data body))))))))

(deftest get-draftset-request-for-non-existent-draftset
  (let [response (route {:uri  "/draftset/missing" :request-method :get})]
    (assert-is-not-found-response response)))

(deftest append-quad-data-with-valid-content-type-to-draftset
  (let [data-file-path "test/resources/test-draftset.trig"
        quads (statements data-file-path)
        draftset-location (create-draftset-through-api)]
    (append-quads-to-draftset-through-api draftset-location quads)
    (let [draftset-graphs (key-set (:data (get-draftset-info-through-api draftset-location)))
          graph-statements (group-by context quads)]
      (doseq [[live-graph graph-quads] graph-statements]
        (let [graph-triples (get-draftset-graph-triples-through-api draftset-location live-graph false)
              expected-statements (map map->Triple graph-quads)]
          (is (contains? draftset-graphs live-graph))
          (is (set expected-statements) (set graph-triples)))))))

(deftest append-quad-data-to-graph-which-exists-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        live-quads (map (comp first second) grouped-quads)
        quads-to-add (rest (second (first grouped-quads)))
        draftset-location (create-draftset-through-api)]
    (publish-quads-through-api route live-quads)
    (append-quads-to-draftset-through-api draftset-location quads-to-add)

    ;;draftset itself should contain the live quads from the graph
    ;;added to along with the quads explicitly added. It should
    ;;not contain any quads from the other live graph.
    (let [draftset-quads (get-draftset-quads-through-api draftset-location false)
          expected-quads (eval-statements (second (first grouped-quads)))]
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest append-quad-data-to-draftset-with-content-type-set-for-request
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api)
          file-part {:tempfile fs :filename "test-draftset.trig"}
          request (-> (append-to-draftset-request draftset-location file-part)
                      (assoc-in [:params :content-type] "application/x-trig"))
          response (route request)]
      (await-success finished-jobs (:finished-job (:body response))))))

(deftest append-triple-data-to-draftset-test
  (with-open [fs (io/input-stream "test/test-triple.nt")]
    (let [draftset-location (create-draftset-through-api)
          file-part {:tempfile fs :filename "test-triple.nt" :content-type "application/n-triples"}
          request (append-to-draftset-request draftset-location file-part)
          response (route request)]
      (is (is-client-error-response? response)))))

(deftest append-triples-to-graph-which-exists-in-live
  (let [[graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api)]
    (publish-quads-through-api route [(first graph-quads)])
    (append-triples-to-draftset-through-api draftset-location (rest graph-quads) graph)

    (let [draftset-graph-triples (get-draftset-graph-triples-through-api draftset-location graph false)
          expected-triples (eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(deftest append-quad-data-without-content-type-to-draftset
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api)
          file-part {:tempfile fs :filename "test-dataset.trig"}
          request (append-to-draftset-request draftset-location file-part)
          response (route request)]
      (is (is-client-error-response? response)))))

(deftest append-data-to-non-existent-draftset
  (let [append-response (make-append-data-to-draftset-request "/draftset/missing" "test/resources/test-draftset.trig")]
    (assert-is-not-found-response append-response)))

(deftest delete-quads-from-live-graphs-in-draftset
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        to-delete (map (comp first second) grouped-quads)
        draftset-location (create-draftset-through-api)]
    (publish-quads-through-api route quads)

    (let [{graph-info :data :as draftset-info} (delete-quads-through-api draftset-location to-delete)
          ds-graphs (keys graph-info)
          expected-graphs (map first grouped-quads)
          draftset-quads (get-draftset-quads-through-api draftset-location false)
          expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))]
      (is (= (set expected-graphs) (set ds-graphs)))
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest delete-quads-from-graph-not-in-live
  (let [draftset-location (create-draftset-through-api)
        to-delete [(->Quad "http://s1" "http://p1" "http://o1" "http://missing-graph1")
                   (->Quad "http://s2" "http://p2" "http://o2" "http://missing-graph2")]
        draftset-info (delete-quads-through-api draftset-location to-delete)]
    (is (empty? (keys (:data draftset-info))))))

(deftest delete-quads-only-in-draftset
  (let [draftset-location (create-draftset-through-api)
        draftset-quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context draftset-quads)]

    (append-quads-to-draftset-through-api draftset-location draftset-quads)
    
    (let [
          ;;NOTE: input data should contain at least two statements in each graph!
          ;;delete one quad from each, so all graphs will be non-empty after delete operation
          to-delete (map (comp first second) grouped-quads)
          draftset-info (delete-quads-through-api draftset-location to-delete)
          expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))
          actual-quads (get-draftset-quads-through-api draftset-location false)]
      (is (= (set expected-quads) (set actual-quads))))))

(deftest delete-all-quads-from-draftset-graph
  (let [draftset-location (create-draftset-through-api)
        initial-statements (statements "test/resources/test-draftset.trig")
        grouped-statements (group-by context initial-statements)
        [graph graph-statements] (first grouped-statements)]
    (append-data-to-draftset-through-api draftset-location "test/resources/test-draftset.trig")

    (let [draftset-info (delete-quads-through-api draftset-location graph-statements)
          expected-graphs (set (map :c initial-statements))
          draftset-graphs (key-set (:data draftset-info))]
      ;;graph should still be in draftset even if it is empty since it should be deleted on publish
      (is (= expected-graphs draftset-graphs)))))

(deftest delete-triples-from-graph-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph graph-quads] (first grouped-quads)
        draftset-location (create-draftset-through-api)]

    (publish-quads-through-api route quads)
    (let [draftset-info (delete-quads-through-api draftset-location [(first graph-quads)])
          draftset-quads (get-draftset-quads-through-api draftset-location false)
          expected-quads (eval-statements (rest graph-quads))]
      (is (= #{live-graph} (key-set (:data draftset-info))))
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest delete-triples-from-graph-not-in-live
  (let [draftset-location (create-draftset-through-api)
        to-delete [(->Triple "http://s1" "http://p1" "http://o1")
                   (->Triple "http://s2" "http://p2" "http://o2")]
        draftset-info (delete-draftset-triples-through-api draftset-location to-delete "http://missing")
        draftset-quads (get-draftset-quads-through-api draftset-location false)]

    ;;graph should not exist in draftset since it was not in live
    (is (empty? (:data draftset-info)))
    (is (empty? draftset-quads))))

(deftest delete-graph-triples-only-in-draftset
  (let [draftset-location (create-draftset-through-api)
        draftset-quads (set (statements "test/resources/test-draftset.trig"))
        [graph graph-quads] (first (group-by context draftset-quads))
        quads-to-delete (take 2 graph-quads)
        triples-to-delete (map map->Triple quads-to-delete)]
    
    (append-data-to-draftset-through-api draftset-location "test/resources/test-draftset.trig")

    (let [draftset-info (delete-draftset-triples-through-api draftset-location triples-to-delete graph)
          quads-after-delete (set (get-draftset-quads-through-api draftset-location))
          expected-quads (set (eval-statements (set/difference draftset-quads quads-to-delete)))]
      (is (= expected-quads quads-after-delete)))))

(deftest delete-all-triples-from-graph
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [graph graph-quads] (first grouped-quads)
        triples-to-delete (map map->Triple graph-quads)
        draftset-location (create-draftset-through-api)
        draftset-quads (set (statements "test/resources/test-draftset.trig"))]
    
    (publish-quads-through-api route quads)

    (let [draftset-info (delete-draftset-triples-through-api draftset-location triples-to-delete graph)
          draftset-quads (get-draftset-quads-through-api draftset-location false)
          draftset-graphs (key-set (:data draftset-info))]

      (is (= #{graph} draftset-graphs))
      (is (empty? draftset-quads)))))

(deftest delete-draftset-triples-request-without-graph-parameter
  (let [draftset-location (create-draftset-through-api)
        draftset-quads (statements "test/resources/test-draftset.trig")]
    (append-data-to-draftset-through-api draftset-location "test/resources/test-draftset.trig")

    (with-open [input-stream (statements->input-stream (take 2 draftset-quads) formats/rdf-ntriples)]
      (let [file-part {:tempfile input-stream :filename "to-delete.nt" :content-type "application/n-triples"}
            delete-request {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}}
            delete-response (route delete-request)]
        (assert-is-not-acceptable-response delete-response)))))

(deftest delete-draftset-data-for-non-existent-draftset
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [file-part {:tempfile fs :filename "to-delete.trig" :content-type "application/x-trig"}
          delete-request {:uri "/draftset/missing/data" :request-method :delete :params {:file file-part}}
          delete-response (route delete-request)]
      (assert-is-not-found-response delete-response))))

(deftest delete-draftset-data-request-with-invalid-rdf-serialisation
  (let [draftset-location (create-draftset-through-api)
        input-stream (ByteArrayInputStream. (.getBytes "not nquads!" "UTF-8"))
        file-part {:tempfile input-stream :filename "to-delete.nq" :content-type "text/x-nquads"}
        delete-request {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}}
        delete-response (route delete-request)]
    (assert-is-unprocessable-response delete-response)))

(deftest delete-draftset-data-request-with-unknown-content-type
  (with-open [input-stream (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api)
          delete-request (create-delete-quads-request draftset-location input-stream "application/unknown-quads-format")
          delete-response (route delete-request)]
      (assert-is-unsupported-media-type-response delete-response))))

(deftest delete-non-existent-live-graph-in-draftset
  (let [draftset-location (create-draftset-through-api)
        graph-to-delete "http://live-graph"]
    (delete-draftset-graph-through-api draftset-location graph-to-delete)

    (let [draftset-info (get-draftset-info-through-api draftset-location)]
      ;;graph to delete should NOT exist in the draftset since it did not exist in live
      ;;at the time of the delete
      (is (= #{} (set (keys (:data draftset-info))))))))

(deftest delete-live-graph-not-in-draftset
  (let [quads (statements "test/resources/test-draftset.trig")
        graph-quads (group-by context quads)
        live-graphs (keys graph-quads)
        graph-to-delete (first live-graphs)
        draftset-location (create-draftset-through-api)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api draftset-location graph-to-delete)

    (let [{draftset-graphs :data} (get-draftset-info-through-api draftset-location)]
      (is (= #{graph-to-delete} (set (keys draftset-graphs)))))))

(deftest delete-graph-with-changes-in-draftset
  (let [draftset-location (create-draftset-through-api)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        published-quad (first graph-quads)
        added-quads (rest graph-quads)]
    (publish-quads-through-api route [published-quad])
    (append-quads-to-draftset-through-api draftset-location added-quads)
    (delete-draftset-graph-through-api draftset-location graph)

    (let [{draftset-graphs :data} (get-draftset-info-through-api draftset-location)]
      (is (= #{graph} (set (keys draftset-graphs)))))))

(deftest delete-graph-only-in-draftset
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api)
        draftset-quads (statements rdf-data-file)
        grouped-quads (group-by context draftset-quads)
        [graph _] (first grouped-quads)]
    (append-data-to-draftset-through-api draftset-location rdf-data-file)

    (delete-draftset-graph-through-api draftset-location graph)
    
    (let [remaining-quads (eval-statements (get-draftset-quads-through-api draftset-location))
          expected-quads (eval-statements (mapcat second (rest grouped-quads)))]
      (is (= (set expected-quads) (set remaining-quads))))

    (let [draftset-info (get-draftset-info-through-api draftset-location)
          draftset-graphs (keys (:data draftset-info))
          expected-graphs (keys grouped-quads)]
      (is (= (set expected-graphs) (set draftset-graphs))))))

(deftest delete-graph-request-for-non-existent-draftset
  (let [delete-graph-request {:uri "/draftset/missing/graph" :request-method :delete :params {:graph "http://some-graph"}}
        response (route delete-graph-request)]
    (assert-is-not-found-response response)))

(deftest publish-draftset-with-graphs-not-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        draftset-location (create-draftset-through-api)]
    (append-quads-to-draftset-through-api draftset-location quads)
    (publish-draftset-through-api draftset-location)

    (let [live-quads (get-live-quads-through-api)]
      (is (= (set (eval-statements quads)) (set live-quads))))))

(deftest publish-draftset-with-statements-added-to-graphs-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (create-draftset-through-api)
        initial-live-quads (map (comp first second) grouped-quads)
        appended-quads (mapcat (comp rest second) grouped-quads)]
    
    (publish-quads-through-api route initial-live-quads)
    (append-quads-to-draftset-through-api draftset-location appended-quads)
    (publish-draftset-through-api draftset-location)

    (let [after-publish-quads (get-live-quads-through-api)]
      (is (= (set (eval-statements quads)) (set after-publish-quads))))))

(deftest publish-draftset-with-statements-deleted-from-graphs-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (create-draftset-through-api)
        to-delete (map (comp first second) grouped-quads)]
    (publish-quads-through-api route quads)
    (delete-quads-through-api draftset-location to-delete)
    (publish-draftset-through-api draftset-location)

    (let [after-publish-quads (get-live-quads-through-api)
          expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))]
      (is (= (set expected-quads) (set after-publish-quads))))))

(deftest publish-draftset-with-graph-deleted-from-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (create-draftset-through-api)
        graph-to-delete (ffirst grouped-quads)
        expected-quads (eval-statements (mapcat second (rest grouped-quads)))]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api draftset-location graph-to-delete)
    (publish-draftset-through-api draftset-location)
    (assert-live-quads expected-quads)))

(deftest publish-draftset-with-deletes-and-appends-from-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph initial-quads] (first grouped-quads)
        draftset-location (create-draftset-through-api)
        to-add (take 2 (second (second grouped-quads)))
        to-delete (take 1 initial-quads)
        expected-quads (eval-statements (set/difference (set/union (set initial-quads) (set to-add)) (set to-delete)))]

    (publish-quads-through-api route initial-quads)
    (append-quads-to-draftset-through-api draftset-location to-add)
    (delete-quads-through-api draftset-location to-delete)
    (publish-draftset-through-api draftset-location)

    (assert-live-quads expected-quads)))

(deftest publish-draftest-with-deletions-from-graphs-not-yet-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [graph graph-quads] (first grouped-quads)
        draftset-location (create-draftset-through-api)]

    ;;delete quads in draftset before they exist in live
    (delete-quads-through-api draftset-location graph-quads)

    ;;add to live then publish draftset
    (publish-quads-through-api route graph-quads)
    (publish-draftset-through-api draftset-location)

    ;;graph should still exist in live
    (assert-live-quads graph-quads)))

(deftest publish-non-existent-draftset
  (let [response (route {:uri "/draftset/missing/publish" :request-method :post})]
    (assert-is-not-found-response response)))

(deftest delete-draftset-test
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api)
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

(deftest query-draftset-with-data
  (let [draftset-location (create-draftset-through-api)
        draftset-data-file "test/resources/test-draftset.trig"
        append-response (make-append-data-to-draftset-request draftset-location draftset-data-file)]
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

(deftest query-draftset-unioned-with-live
  (let [test-quads (statements "test/resources/test-draftset.trig")
        grouped-test-quads (group-by context test-quads)
        [live-graph live-quads] (first grouped-test-quads)
        [ds-live-graph draftset-quads] (second grouped-test-quads)
        draftset-location (create-draftset-through-api)]

    (publish-quads-through-api route live-quads)
    (append-quads-to-draftset-through-api draftset-location draftset-quads)

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

(deftest query-non-existent-draftset
  (let [response (route {:uri "/draftset/missing/query" :params {:query "SELECT * WHERE { ?s ?p ?o }"} :request-method :post})]
    (assert-is-not-found-response response)))

(deftest query-draftest-request-with-missing-query-parameter
  (let [draftset-location (create-draftset-through-api)
        response (route {:uri (str draftset-location "/query") :request-method :post})]
    (assert-is-not-acceptable-response response)))

(deftest query-draftset-request-with-invalid-http-method
  (let [draftset-location (create-draftset-through-api)
        query-request {:uri (str draftset-location "/query")
                       :request-method :put
                       :headers {"Accept" "text/plain"}
                       :params {:query "SELECT * WHERE { ?s ?p ?o }"}}
        response (route query-request)]
    (assert-is-method-not-allowed-response response)))

(deftest get-draftset-graph-triples-data
  (let [draftset-location (create-draftset-through-api)
        draftset-data-file "test/resources/test-draftset.trig"
        input-quads (statements draftset-data-file)]
    (append-quads-to-draftset-through-api draftset-location input-quads)

    (doseq [[graph quads] (group-by context input-quads)]
      (let [graph-triples (set (eval-statements (map map->Triple quads)))
            response-triples (set (get-draftset-graph-triples-through-api draftset-location graph false))]
        (is (= graph-triples response-triples))))))

(deftest get-draftset-quads-data
  (let [draftset-location (create-draftset-through-api)
        draftset-data-file "test/resources/test-draftset.trig"]
    (append-data-to-draftset-through-api draftset-location draftset-data-file)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location))
          input-quads (set (eval-statements (statements draftset-data-file)))]
      (is (= input-quads response-quads)))))

(deftest get-draftset-quads-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (first (keys grouped-quads))
        draftset-location (create-draftset-through-api)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api draftset-location graph-to-delete)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location true))
          expected-quads (set (eval-statements (mapcat second (rest grouped-quads))))]
      (is (= expected-quads response-quads)))))

(deftest get-added-draftset-quads-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph live-quads] (first grouped-quads)
        [draftset-graph draftset-quads] (second grouped-quads)
        draftset-location (create-draftset-through-api)]
    (publish-quads-through-api route live-quads)
    (append-quads-to-draftset-through-api draftset-location draftset-quads)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location true))
          expected-quads (set (eval-statements (concat live-quads draftset-quads)))]
      (is (= expected-quads response-quads)))))

(deftest get-draftset-triples-for-deleted-graph-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (ffirst grouped-quads)
        draftset-location (create-draftset-through-api)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api draftset-location graph-to-delete)

    (let [draftset-triples (get-draftset-graph-triples-through-api draftset-location graph-to-delete true)]
      (is (empty? draftset-triples)))))

(deftest get-draftset-triples-for-published-graph-not-in-draftset-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        [graph graph-quads] (first (group-by context quads))
        draftset-location (create-draftset-through-api)]
    (publish-quads-through-api route graph-quads)

    (let [draftset-graph-triples (get-draftset-graph-triples-through-api draftset-location graph true)
          expected-triples (eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(deftest get-draftset-graph-triples-request-without-graph
  (let [draftset-location (create-draftset-through-api)]
    (append-quads-to-draftset-through-api draftset-location (statements "test/resources/test-draftset.trig"))
    (let [data-request {:uri (str draftset-location "/data")
                        :request-method :get
                        :headers {"Accept" "application/n-triples"}}
          data-response (route data-request)]
      (assert-is-not-acceptable-response data-response))))

(deftest get-drafset-data-for-missing-draftset
  (let [response (route {:uri "/draftset/missing/data" :request-method :get :headers {"Accept" "application/n-quads"}})]
    (assert-is-not-found-response response)))

(deftest set-draftset-with-existing-title-and-description-metadata
  (let [draftset-location (create-draftset-through-api "Test draftset" "Test description")
        new-title "Updated title"
        new-description "Updated description"
        meta-request {:uri (str draftset-location "/meta") :request-method :put :params {:display-name new-title :description new-description}}
        {:keys [body] :as  meta-response} (route meta-request)]
    
    (assert-is-ok-response meta-response)
    (assert-schema draftset-info-schema body)

    (is (= new-title (:display-name body)))
    (is (= new-description (:description body)))))

(deftest set-missing-draftset-metadata
  (let [meta-request {:uri "/draftset/missing/meta" :request-method :put :params {:display-name "Title!" :description "Description"}}
        meta-response (route meta-request)]
    (assert-is-not-found-response meta-response)))

(defn- setup-route [test-function]
  (binding [*route* (draftset-api-routes *test-backend*)]
    (test-function)))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each setup-route)
(use-fixtures :each (fn [tf]
                      (wrap-clean-test-db #(setup-route tf))))
