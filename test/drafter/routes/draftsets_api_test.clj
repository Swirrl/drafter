(ns drafter.routes.draftsets-api-test
  (:require [drafter.test-common :refer [*test-backend* test-triples wrap-clean-test-db wrap-db-setup
                                         stream->string select-all-in-graph make-graph-live!
                                         import-data-to-draft! await-success key-set test-editor test-publisher
                                         test-manager]]
            [clojure.test :refer :all]
            [clojure.set :as set]
            [drafter.routes.draftsets-api :refer :all]
            [drafter.user :as user]
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

(defn- with-identity [user request]
  (assoc request :identity user))

(defn- append-to-draftset-request [user draftset-location file-part]
  (with-identity user
    {:uri (str draftset-location "/data")
     :request-method :post
     :params {:file file-part}}))

(defn- create-draftset-request
  ([] (create-draftset-request test-editor))
  ([user] (create-draftset-request user nil))
  ([user display-name] (create-draftset-request user display-name nil))
  ([user display-name description]
   (with-identity user {:uri "/draftset" :request-method :post :params {:display-name display-name :description description}})))

(defn- make-append-data-to-draftset-request [user draftset-location data-file-path]
  (with-open [fs (io/input-stream data-file-path)]
    (let [file-part {:tempfile fs :filename "test-dataset.trig" :content-type "application/x-trig"}
          request (append-to-draftset-request user draftset-location file-part)]
      (route request))))

(defn- append-data-to-draftset-through-api [user draftset-location draftset-data-file]
  (let [append-response (make-append-data-to-draftset-request user draftset-location draftset-data-file)]
    (await-success finished-jobs (:finished-job (:body append-response)))))

(defn- statements->append-request [user draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)
        file-part {:tempfile input-stream :filename (str "data." (.getDefaultFileExtension format)) :content-type (.getDefaultMIMEType format)}]
    (append-to-draftset-request user draftset-location file-part)))

(defn- append-quads-to-draftset-through-api [user draftset-location quads]
  (let [request (statements->append-request user draftset-location quads formats/rdf-nquads)
        response (route request)]
    (await-success finished-jobs (get-in response [:body :finished-job]))))

(defn- statements->append-triples-request [user draftset-location triples graph]
  (-> (statements->append-request user draftset-location triples formats/rdf-ntriples)
      (assoc-in [:params :graph] graph)))

(defn- append-triples-to-draftset-through-api [user draftset-location triples graph]
  (let [request (statements->append-triples-request user draftset-location triples graph)
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
   :created-at Date
   :created-by s/Str
   :current-owner s/Str})

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

(defn assert-is-forbidden-response [response]
  (assert-schema (response-code-schema 403) response))

(defn assert-is-bad-request-response [response]
  (assert-schema (response-code-schema 400) response))

(defn- eval-statement [s]
  (util/map-values str s))

(defn- eval-statements [ss]
  (map eval-statement ss))

(defn- concrete-statements [source format]
  (eval-statements (statements source :format format)))

(defn- create-draftset-through-api
  ([] (create-draftset-through-api test-editor))
  ([user] (create-draftset-through-api user nil))
  ([user display-name] (create-draftset-through-api user display-name nil))
  ([user display-name description]
   (let [request (create-draftset-request user display-name description)
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

(defn- create-publish-request [draftset-location user]
  (with-identity user {:uri (str draftset-location "/publish") :request-method :post}))

(defn- publish-draftset-through-api [draftset-location user]
  (let [publish-request (create-publish-request draftset-location user)
        publish-response (route publish-request)]
    (await-success finished-jobs (:finished-job (:body publish-response)))))

(defn- publish-quads-through-api [route quads]
  (let [draftset-location (create-draftset-through-api test-publisher)]
    (append-quads-to-draftset-through-api test-publisher draftset-location quads)
    (publish-draftset-through-api draftset-location test-publisher)))

(defn- create-delete-quads-request [user draftset-location input-stream format]
  (let [file-part {:tempfile input-stream :filename "to-delete.nq" :content-type format}]
    (with-identity user {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}})))

(defn- get-draftset-info-request [draftset-location user]
  (with-identity user {:uri draftset-location :request-method :get}))

(defn- get-draftset-info-through-api [draftset-location user]
  (let [{:keys [body] :as response} (route (get-draftset-info-request draftset-location user))]
    (assert-is-ok-response response)
    (assert-schema draftset-info-schema body)
    body))

(defn- delete-draftset-graph-request [user draftset-location graph-to-delete]
  (with-identity user {:uri (str draftset-location "/graph") :request-method :delete :params {:graph graph-to-delete}}))

(defn- delete-draftset-graph-through-api [user draftset-location graph-to-delete]
  (let [delete-graph-request (delete-draftset-graph-request user draftset-location graph-to-delete)
        delete-graph-response (route delete-graph-request)]
    (assert-is-ok-response delete-graph-response)))

(defn- await-delete-statements-response [response]
  (let [job-result (await-success finished-jobs (get-in response [:body :finished-job]))]
    (:draftset job-result)))

(defn- create-delete-statements-request [user draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)
        file-name (str "to-delete." (.getDefaultFileExtension format))
        file-part {:tempfile input-stream :filename file-name  :content-type (.getDefaultMIMEType format)}]
    (with-identity user {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}})))

(defn- delete-quads-through-api [user draftset-location quads]
  (let [delete-request (create-delete-statements-request user draftset-location quads formats/rdf-nquads)
        delete-response (route delete-request)]
    (await-delete-statements-response delete-response)))

(defn- delete-draftset-triples-through-api [user draftset-location triples graph]
  (let [delete-request (create-delete-statements-request user draftset-location triples formats/rdf-ntriples)
        delete-request (assoc-in delete-request [:params :graph] graph)
        delete-response (route delete-request)]
    (await-delete-statements-response delete-response)))

;;TODO: Get quads through query of live endpoint? This depends on
;;'union with live' working correctly
(defn- get-live-quads-through-api []
  (let [tmp-ds (create-draftset-through-api)]
    (get-draftset-quads-through-api tmp-ds true)))

(defn- assert-live-quads [expected-quads]
  (let [live-quads (get-live-quads-through-api)]
    (is (= (set (eval-statements expected-quads)) (set live-quads)))))

(deftest create-draftset-without-title-or-description
  (let [response (route (with-identity test-editor {:uri "/draftset" :request-method :post}))]
    (assert-is-see-other-response response)))

(deftest create-draftset-with-title-and-without-description
  (let [response (route (create-draftset-request test-editor "Test Title!"))]
    (assert-is-see-other-response response)))

(deftest create-draftset-with-title-and-description
  (let [response (route (create-draftset-request test-editor "Test title" "Test description"))]
    (assert-is-see-other-response response)))

(deftest get-all-draftsets-test
  (let [draftset-count 10
        titles (map #(str "Title" %) (range 1 (inc draftset-count)))
        create-requests (map #(create-draftset-request test-editor %) titles)
        create-responses (doall (map route create-requests))]
    (doseq [r create-responses]
      (assert-is-see-other-response r))

    ;;create another draftset owned by a different user - this should
    ;;not be returned in the results
    (create-draftset-through-api test-publisher "Other draftset")

    (let [get-all-request (with-identity test-editor {:uri "/draftsets" :request-method :get})
          {:keys [body] :as response} (route get-all-request)]
      (assert-is-ok-response response)
      
      (is (= draftset-count (count body)))
      (assert-schema [draftset-without-description-info-schema] body)

      (let [returned-names (map :display-name body)]
        (is (= (set returned-names) (set titles)))))))

(deftest get-empty-draftset-without-title-or-description
  (let [draftset-location (create-draftset-through-api test-editor)
        ds-info (get-draftset-info-through-api draftset-location test-editor)]
    (assert-schema draftset-without-title-or-description-info-schema ds-info)))

(deftest get-empty-draftset-without-description
  (let [display-name "Test title!"
        draftset-location (create-draftset-through-api test-editor display-name)
        ds-info (get-draftset-info-through-api draftset-location test-editor)]
    (assert-schema draftset-without-description-info-schema ds-info)
    (is (= display-name (:display-name ds-info)))))

(deftest get-empty-draftset-with-description
  (let [display-name "Test title!"
        description "Draftset used in a test"
        draftset-location (create-draftset-through-api test-editor display-name description)]
    
    (let [ds-info (get-draftset-info-through-api draftset-location test-editor)]
      (assert-schema draftset-with-description-info-schema ds-info)
      (is (= display-name (:display-name ds-info)))
      (is (= description (:description ds-info))))))

(deftest get-draftset-containing-data
  (let [display-name "Test title!"
        draftset-location (create-draftset-through-api test-editor display-name)
        quads (statements "test/resources/test-draftset.trig")
        live-graphs (set (keys (group-by context quads)))]
    (append-quads-to-draftset-through-api test-editor draftset-location quads)
    
    (let [ds-info (get-draftset-info-through-api draftset-location test-editor)]
      (assert-schema draftset-without-description-info-schema ds-info)
      
      (is (= display-name (:display-name ds-info)))
      (is (= live-graphs (key-set (:data ds-info)))))))

(deftest get-draftset-request-for-non-existent-draftset
  (let [response (route (get-draftset-info-request "/draftset/missing" test-publisher))]
    (assert-is-not-found-response response)))

(deftest get-draftset-for-other-user-test
  (let [draftset-location (create-draftset-through-api test-editor)
        get-request (get-draftset-info-request draftset-location test-publisher)
        get-response (route get-request)]
    (assert-is-forbidden-response get-response)))

(deftest append-quad-data-with-valid-content-type-to-draftset
  (let [data-file-path "test/resources/test-draftset.trig"
        quads (statements data-file-path)
        draftset-location (create-draftset-through-api test-editor)]
    (append-quads-to-draftset-through-api test-editor draftset-location quads)
    (let [draftset-graphs (key-set (:data (get-draftset-info-through-api draftset-location test-editor)))
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
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api route live-quads)
    (append-quads-to-draftset-through-api test-editor draftset-location quads-to-add)

    ;;draftset itself should contain the live quads from the graph
    ;;added to along with the quads explicitly added. It should
    ;;not contain any quads from the other live graph.
    (let [draftset-quads (get-draftset-quads-through-api draftset-location false)
          expected-quads (eval-statements (second (first grouped-quads)))]
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest append-quad-data-to-draftset-with-content-type-set-for-request
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api test-editor)
          file-part {:tempfile fs :filename "test-draftset.trig"}
          request (-> (append-to-draftset-request test-editor draftset-location file-part)
                      (assoc-in [:params :content-type] "application/x-trig"))
          response (route request)]
      (await-success finished-jobs (:finished-job (:body response))))))

(deftest append-triple-data-to-draftset-test
  (with-open [fs (io/input-stream "test/test-triple.nt")]
    (let [draftset-location (create-draftset-through-api test-editor)
          file-part {:tempfile fs :filename "test-triple.nt" :content-type "application/n-triples"}
          request (append-to-draftset-request test-editor draftset-location file-part)
          response (route request)]
      (is (is-client-error-response? response)))))

(deftest append-triples-to-graph-which-exists-in-live
  (let [[graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api route [(first graph-quads)])
    (append-triples-to-draftset-through-api test-editor draftset-location (rest graph-quads) graph)

    (let [draftset-graph-triples (get-draftset-graph-triples-through-api draftset-location graph false)
          expected-triples (eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(deftest append-quad-data-without-content-type-to-draftset
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api test-editor)
          file-part {:tempfile fs :filename "test-dataset.trig"}
          request (append-to-draftset-request test-editor draftset-location file-part)
          response (route request)]
      (is (is-client-error-response? response)))))

(deftest append-data-to-non-existent-draftset
  (let [append-response (make-append-data-to-draftset-request test-publisher "/draftset/missing" "test/resources/test-draftset.trig")]
    (assert-is-not-found-response append-response)))

(deftest append-quads-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        quads (statements "test/resources/test-draftset.trig")
        append-request (statements->append-request test-publisher draftset-location quads formats/rdf-nquads)
        append-response (route append-request)]
    (assert-is-forbidden-response append-response)))

(deftest append-graph-triples-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        append-request (statements->append-triples-request test-publisher draftset-location graph-quads graph)
        append-response (route append-request)]
    (assert-is-forbidden-response append-response)))

(deftest delete-quads-from-live-graphs-in-draftset
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        to-delete (map (comp first second) grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api route quads)

    (let [{graph-info :data :as draftset-info} (delete-quads-through-api test-editor draftset-location to-delete)
          ds-graphs (keys graph-info)
          expected-graphs (map first grouped-quads)
          draftset-quads (get-draftset-quads-through-api draftset-location false)
          expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))]
      (is (= (set expected-graphs) (set ds-graphs)))
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest delete-quads-from-graph-not-in-live
  (let [draftset-location (create-draftset-through-api test-editor)
        to-delete [(->Quad "http://s1" "http://p1" "http://o1" "http://missing-graph1")
                   (->Quad "http://s2" "http://p2" "http://o2" "http://missing-graph2")]
        draftset-info (delete-quads-through-api test-editor draftset-location to-delete)]
    (is (empty? (keys (:data draftset-info))))))

(deftest delete-quads-only-in-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context draftset-quads)]

    (append-quads-to-draftset-through-api test-editor draftset-location draftset-quads)
    
    (let [
          ;;NOTE: input data should contain at least two statements in each graph!
          ;;delete one quad from each, so all graphs will be non-empty after delete operation
          to-delete (map (comp first second) grouped-quads)
          draftset-info (delete-quads-through-api test-editor draftset-location to-delete)
          expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))
          actual-quads (get-draftset-quads-through-api draftset-location false)]
      (is (= (set expected-quads) (set actual-quads))))))

(deftest delete-all-quads-from-draftset-graph
  (let [draftset-location (create-draftset-through-api test-editor)
        initial-statements (statements "test/resources/test-draftset.trig")
        grouped-statements (group-by context initial-statements)
        [graph graph-statements] (first grouped-statements)]
    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")

    (let [draftset-info (delete-quads-through-api test-editor draftset-location graph-statements)
          expected-graphs (set (map :c initial-statements))
          draftset-graphs (key-set (:data draftset-info))]
      ;;graph should still be in draftset even if it is empty since it should be deleted on publish
      (is (= expected-graphs draftset-graphs)))))

(deftest delete-triples-from-graph-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph graph-quads] (first grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]

    (publish-quads-through-api route quads)
    (let [draftset-info (delete-quads-through-api test-editor draftset-location [(first graph-quads)])
          draftset-quads (get-draftset-quads-through-api draftset-location false)
          expected-quads (eval-statements (rest graph-quads))]
      (is (= #{live-graph} (key-set (:data draftset-info))))
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest delete-triples-from-graph-not-in-live
  (let [draftset-location (create-draftset-through-api test-editor)
        to-delete [(->Triple "http://s1" "http://p1" "http://o1")
                   (->Triple "http://s2" "http://p2" "http://o2")]
        draftset-info (delete-draftset-triples-through-api test-editor draftset-location to-delete "http://missing")
        draftset-quads (get-draftset-quads-through-api draftset-location false)]

    ;;graph should not exist in draftset since it was not in live
    (is (empty? (:data draftset-info)))
    (is (empty? draftset-quads))))

(deftest delete-graph-triples-only-in-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-quads (set (statements "test/resources/test-draftset.trig"))
        [graph graph-quads] (first (group-by context draftset-quads))
        quads-to-delete (take 2 graph-quads)
        triples-to-delete (map map->Triple quads-to-delete)]
    
    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")

    (let [draftset-info (delete-draftset-triples-through-api test-editor draftset-location triples-to-delete graph)
          quads-after-delete (set (get-draftset-quads-through-api draftset-location))
          expected-quads (set (eval-statements (set/difference draftset-quads quads-to-delete)))]
      (is (= expected-quads quads-after-delete)))))

(deftest delete-all-triples-from-graph
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [graph graph-quads] (first grouped-quads)
        triples-to-delete (map map->Triple graph-quads)
        draftset-location (create-draftset-through-api test-editor)
        draftset-quads (set (statements "test/resources/test-draftset.trig"))]
    
    (publish-quads-through-api route quads)

    (let [draftset-info (delete-draftset-triples-through-api test-editor draftset-location triples-to-delete graph)
          draftset-quads (get-draftset-quads-through-api draftset-location false)
          draftset-graphs (key-set (:data draftset-info))]

      (is (= #{graph} draftset-graphs))
      (is (empty? draftset-quads)))))

(deftest delete-draftset-triples-request-without-graph-parameter
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-quads (statements "test/resources/test-draftset.trig")]
    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")

    (with-open [input-stream (statements->input-stream (take 2 draftset-quads) formats/rdf-ntriples)]
      (let [file-part {:tempfile input-stream :filename "to-delete.nt" :content-type "application/n-triples"}
            delete-request (with-identity test-editor {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}})
            delete-response (route delete-request)]
        (assert-is-not-acceptable-response delete-response)))))

(deftest delete-draftset-data-for-non-existent-draftset
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [file-part {:tempfile fs :filename "to-delete.trig" :content-type "application/x-trig"}
          delete-request {:uri "/draftset/missing/data" :request-method :delete :params {:file file-part}}
          delete-response (route delete-request)]
      (assert-is-not-found-response delete-response))))

(deftest delete-draftset-data-request-with-invalid-rdf-serialisation
  (let [draftset-location (create-draftset-through-api test-editor)
        input-stream (ByteArrayInputStream. (.getBytes "not nquads!" "UTF-8"))
        file-part {:tempfile input-stream :filename "to-delete.nq" :content-type "text/x-nquads"}
        delete-request (with-identity test-editor {:uri (str draftset-location "/data") :request-method :delete :params {:file file-part}})
        delete-response (route delete-request)]
    (assert-is-unprocessable-response delete-response)))

(deftest delete-draftset-data-request-with-unknown-content-type
  (with-open [input-stream (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api test-editor)
          delete-request (create-delete-quads-request test-editor draftset-location input-stream "application/unknown-quads-format")
          delete-response (route delete-request)]
      (assert-is-unsupported-media-type-response delete-response))))

(deftest delete-non-existent-live-graph-in-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        graph-to-delete "http://live-graph"]
    (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)

    (let [draftset-info (get-draftset-info-through-api draftset-location test-editor)]
      ;;graph to delete should NOT exist in the draftset since it did not exist in live
      ;;at the time of the delete
      (is (= #{} (set (keys (:data draftset-info))))))))

(deftest delete-live-graph-not-in-draftset
  (let [quads (statements "test/resources/test-draftset.trig")
        graph-quads (group-by context quads)
        live-graphs (keys graph-quads)
        graph-to-delete (first live-graphs)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)

    (let [{draftset-graphs :data} (get-draftset-info-through-api draftset-location test-editor)]
      (is (= #{graph-to-delete} (set (keys draftset-graphs)))))))

(deftest delete-graph-with-changes-in-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        published-quad (first graph-quads)
        added-quads (rest graph-quads)]
    (publish-quads-through-api route [published-quad])
    (append-quads-to-draftset-through-api test-editor draftset-location added-quads)
    (delete-draftset-graph-through-api test-editor draftset-location graph)

    (let [{draftset-graphs :data} (get-draftset-info-through-api draftset-location test-editor)]
      (is (= #{graph} (set (keys draftset-graphs)))))))

(deftest delete-graph-only-in-draftset
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api test-editor)
        draftset-quads (statements rdf-data-file)
        grouped-quads (group-by context draftset-quads)
        [graph _] (first grouped-quads)]
    (append-data-to-draftset-through-api test-editor draftset-location rdf-data-file)

    (delete-draftset-graph-through-api test-editor draftset-location graph)
    
    (let [remaining-quads (eval-statements (get-draftset-quads-through-api draftset-location))
          expected-quads (eval-statements (mapcat second (rest grouped-quads)))]
      (is (= (set expected-quads) (set remaining-quads))))

    (let [draftset-info (get-draftset-info-through-api draftset-location test-editor)
          draftset-graphs (keys (:data draftset-info))
          expected-graphs (keys grouped-quads)]
      (is (= (set expected-graphs) (set draftset-graphs))))))

(deftest delete-graph-request-for-non-existent-draftset
  (let [delete-graph-request {:uri "/draftset/missing/graph" :request-method :delete :params {:graph "http://some-graph"}}
        response (route delete-graph-request)]
    (assert-is-not-found-response response)))

(deftest delete-graph-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        [graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (append-quads-to-draftset-through-api test-editor draftset-location quads)

    (let [delete-request (delete-draftset-graph-request test-publisher draftset-location graph)
          delete-response (route delete-request)]
      (assert-is-forbidden-response delete-response))))

(deftest publish-draftset-with-graphs-not-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        draftset-location (create-draftset-through-api test-publisher)]
    (append-quads-to-draftset-through-api test-publisher draftset-location quads)
    (publish-draftset-through-api draftset-location test-publisher)

    (let [live-quads (get-live-quads-through-api)]
      (is (= (set (eval-statements quads)) (set live-quads))))))

(deftest publish-draftset-with-statements-added-to-graphs-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (create-draftset-through-api test-publisher)
        initial-live-quads (map (comp first second) grouped-quads)
        appended-quads (mapcat (comp rest second) grouped-quads)]
    
    (publish-quads-through-api route initial-live-quads)
    (append-quads-to-draftset-through-api test-publisher draftset-location appended-quads)
    (publish-draftset-through-api draftset-location test-publisher)

    (let [after-publish-quads (get-live-quads-through-api)]
      (is (= (set (eval-statements quads)) (set after-publish-quads))))))

(deftest publish-draftset-with-statements-deleted-from-graphs-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (create-draftset-through-api test-publisher)
        to-delete (map (comp first second) grouped-quads)]
    (publish-quads-through-api route quads)
    (delete-quads-through-api test-publisher draftset-location to-delete)
    (publish-draftset-through-api draftset-location test-publisher)

    (let [after-publish-quads (get-live-quads-through-api)
          expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))]
      (is (= (set expected-quads) (set after-publish-quads))))))

(deftest publish-draftset-with-graph-deleted-from-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (create-draftset-through-api test-publisher)
        graph-to-delete (ffirst grouped-quads)
        expected-quads (eval-statements (mapcat second (rest grouped-quads)))]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api test-publisher draftset-location graph-to-delete)
    (publish-draftset-through-api draftset-location test-publisher)
    (assert-live-quads expected-quads)))

(deftest publish-draftset-with-deletes-and-appends-from-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph initial-quads] (first grouped-quads)
        draftset-location (create-draftset-through-api test-publisher)
        to-add (take 2 (second (second grouped-quads)))
        to-delete (take 1 initial-quads)
        expected-quads (eval-statements (set/difference (set/union (set initial-quads) (set to-add)) (set to-delete)))]

    (publish-quads-through-api route initial-quads)
    (append-quads-to-draftset-through-api test-publisher draftset-location to-add)
    (delete-quads-through-api test-publisher draftset-location to-delete)
    (publish-draftset-through-api draftset-location test-publisher)

    (assert-live-quads expected-quads)))

(deftest publish-draftest-with-deletions-from-graphs-not-yet-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [graph graph-quads] (first grouped-quads)
        draftset-location (create-draftset-through-api test-publisher)]

    ;;delete quads in draftset before they exist in live
    (delete-quads-through-api test-publisher draftset-location graph-quads)

    ;;add to live then publish draftset
    (publish-quads-through-api route graph-quads)
    (publish-draftset-through-api draftset-location test-publisher)

    ;;graph should still exist in live
    (assert-live-quads graph-quads)))

(deftest publish-non-existent-draftset
  (let [response (route {:uri "/draftset/missing/publish" :request-method :post})]
    (assert-is-not-found-response response)))

(deftest publish-by-non-publisher-test
  (let [draftset-location (create-draftset-through-api test-editor)]
    (append-quads-to-draftset-through-api test-editor draftset-location (statements "test/resources/test-draftset.trig"))
    (let [publish-response (route (create-publish-request draftset-location test-editor))]
      (assert-is-forbidden-response publish-response))))

(deftest publish-by-non-owner-test
  (let [draftset-location (create-draftset-through-api test-publisher)
        quads (statements "test/resources/test-draftset.trig")]
    (append-quads-to-draftset-through-api test-publisher draftset-location quads)
    (let [publish-request (create-publish-request draftset-location test-manager)
          publish-response (route publish-request)]
      (assert-is-forbidden-response publish-response))))

(defn- create-delete-draftset-request [draftset-location user]
  {:uri draftset-location :request-method :delete :identity user})

(deftest delete-draftset-test
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api test-editor)
        delete-response (route (create-delete-draftset-request draftset-location test-editor))]
    (assert-is-ok-response delete-response)
    
    (let [get-response (route {:uri draftset-location :request-method :get})]
      (assert-is-not-found-response get-response))))

(deftest delete-non-existent-draftset-test
  (let [delete-response (route (create-delete-draftset-request "/draftset/missing" test-publisher))]
    (assert-is-not-found-response delete-response)))

(deftest delete-draftset-by-non-owner-test
  (let [draftset-location (create-draftset-through-api test-editor)
        delete-response (route (create-delete-draftset-request draftset-location test-manager))]
    (assert-is-forbidden-response delete-response)))

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

(defn- create-query-request [user draftset-location query accept-content-type & {:keys [union-with-live?]}]
  (with-identity user
    {:uri (str draftset-location "/query")
     :headers {"Accept" accept-content-type}
     :request-method :post
     :params {:query query :union-with-live union-with-live?}}))

(deftest query-draftset-with-data
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-data-file "test/resources/test-draftset.trig"
        append-response (make-append-data-to-draftset-request test-editor draftset-location draftset-data-file)]
    (await-success finished-jobs (:finished-job (:body append-response)) )
    (let [query "CONSTRUCT { ?s ?p ?o }  WHERE { GRAPH ?g { ?s ?p ?o } }"
          query-request (create-query-request test-editor draftset-location query "application/n-triples")
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
        draftset-location (create-draftset-through-api test-editor)]

    (publish-quads-through-api route live-quads)
    (append-quads-to-draftset-through-api test-editor draftset-location draftset-quads)

    (let [query "SELECT * WHERE { GRAPH ?c { ?s ?p ?o } }"
          query-request (create-query-request test-editor draftset-location query "application/sparql-results+json" :union-with-live? true)
          {:keys [body] :as query-response} (route query-request)
          result-state (atom #{})
          result-handler (result-set-handler result-state)
          parser (doto (SPARQLResultsCSVParser.) (.setTupleQueryResultHandler result-handler))]

      (.parse parser body)

      (let [expected-quads (set (eval-statements test-quads))]
        (is (= expected-quads @result-state))))))

(deftest query-non-existent-draftset
  (let [request (create-query-request test-editor "/draftset/missing" "SELECT * WHERE { ?s ?p ?o }" "application/sparql-results+json")
        response (route request)]
    (assert-is-not-found-response response)))

(deftest query-draftest-request-with-missing-query-parameter
  (let [draftset-location (create-draftset-through-api test-editor)
        response (route (with-identity test-editor {:uri (str draftset-location "/query") :request-method :post}))]
    (assert-is-not-acceptable-response response)))

(deftest query-draftset-request-with-invalid-http-method
  (let [draftset-location (create-draftset-through-api test-editor)
        query-request (create-query-request test-editor draftset-location "SELECT * WHERE { ?s ?p ?o }" "text/plain")
        query-request (assoc query-request :request-method :put)
        response (route query-request)]
    (assert-is-method-not-allowed-response response)))

(deftest query-draftset-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        query-request (create-query-request test-publisher draftset-location "SELECT * WHERE { ?s ?p ?o }" "application/sparql-results+json")
        query-response (route query-request)]
    (assert-is-forbidden-response query-response)))

(deftest get-draftset-graph-triples-data
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-data-file "test/resources/test-draftset.trig"
        input-quads (statements draftset-data-file)]
    (append-quads-to-draftset-through-api test-editor draftset-location input-quads)

    (doseq [[graph quads] (group-by context input-quads)]
      (let [graph-triples (set (eval-statements (map map->Triple quads)))
            response-triples (set (get-draftset-graph-triples-through-api draftset-location graph false))]
        (is (= graph-triples response-triples))))))

(deftest get-draftset-quads-data
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-data-file "test/resources/test-draftset.trig"]
    (append-data-to-draftset-through-api test-editor draftset-location draftset-data-file)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location))
          input-quads (set (eval-statements (statements draftset-data-file)))]
      (is (= input-quads response-quads)))))

(deftest get-draftset-quads-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (first (keys grouped-quads))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location true))
          expected-quads (set (eval-statements (mapcat second (rest grouped-quads))))]
      (is (= expected-quads response-quads)))))

(deftest get-added-draftset-quads-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph live-quads] (first grouped-quads)
        [draftset-graph draftset-quads] (second grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api route live-quads)
    (append-quads-to-draftset-through-api test-editor draftset-location draftset-quads)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location true))
          expected-quads (set (eval-statements (concat live-quads draftset-quads)))]
      (is (= expected-quads response-quads)))))

(deftest get-draftset-triples-for-deleted-graph-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (ffirst grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)

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
  (let [draftset-location (create-draftset-through-api test-editor)]
    (append-quads-to-draftset-through-api test-editor draftset-location (statements "test/resources/test-draftset.trig"))
    (let [data-request {:uri (str draftset-location "/data")
                        :request-method :get
                        :headers {"Accept" "application/n-triples"}}
          data-response (route data-request)]
      (assert-is-not-acceptable-response data-response))))

(deftest get-drafset-data-for-missing-draftset
  (let [response (route {:uri "/draftset/missing/data" :request-method :get :headers {"Accept" "application/n-quads"}})]
    (assert-is-not-found-response response)))

(defn- create-update-draftset-metadata-request [user draftset-location title description]
  (with-identity user
    {:uri (str draftset-location "/meta") :request-method :put :params {:display-name title :description description}}))

(defn- update-draftset-metadata-through-api [user draftset-location title description]
  (let [request (create-update-draftset-metadata-request user draftset-location title description)
        {:keys [body] :as response} (route request)]
    (assert-is-ok-response response)
    (assert-schema draftset-info-schema body)
    body))

(deftest set-draftset-with-existing-title-and-description-metadata
  (let [draftset-location (create-draftset-through-api test-editor "Test draftset" "Test description")
        new-title "Updated title"
        new-description "Updated description"
        {:keys [display-name description]} (update-draftset-metadata-through-api test-editor draftset-location new-title new-description)]
    (is (= new-title display-name))
    (is (= new-description description))))

(deftest set-metadata-for-draftset-with-no-title-or-description
  (let [draftset-location (create-draftset-through-api)
        new-title "New title"
        new-description "New description"
        {:keys [display-name description]} (update-draftset-metadata-through-api test-editor draftset-location new-title new-description)]
    (is (= new-title display-name))
    (is (= new-description description))))

(deftest set-missing-draftset-metadata
  (let [meta-request (create-update-draftset-metadata-request test-manager "/draftset/missing" "Title!" "Description")
        meta-response (route meta-request)]
    (assert-is-not-found-response meta-response)))

(deftest set-metadata-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor "Test draftset" "Test description")
        update-request (create-update-draftset-metadata-request test-publisher draftset-location "New title" "New description")
        update-response (route update-request)]
    (assert-is-forbidden-response update-response)))

(defn- create-offer-request [user draftset-location role]
  (with-identity user {:uri (str draftset-location "/offer") :request-method :post :params {:role (name role)}}))

(defn- offer-draftset-through-api [user draftset-location role]
  (let [response (route (create-offer-request user draftset-location role))]
    (assert-is-ok-response response)))

(deftest offer-draftset-test
  (let [draftset-location (create-draftset-through-api test-editor)
        offer-request (create-offer-request test-editor draftset-location :publisher)
        offer-response (route offer-request)]
    (assert-is-ok-response offer-response)

    ;;user should not longer have access after yielding ownership
    (let [get-request (get-draftset-info-request draftset-location test-editor)
          get-response (route get-request)]
      (assert-is-forbidden-response get-response))))

(deftest offer-non-existent-draftset-test
  (let [offer-response (route (create-offer-request test-editor "/draftset/missing" :publisher))]
    (assert-is-not-found-response offer-response)))

(deftest offer-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        offer-response (route (create-offer-request test-publisher draftset-location :manager))]
    (assert-is-forbidden-response offer-response)))

(deftest offer-with-invalid-role
  (let [draftset-location (create-draftset-through-api test-editor)
        offer-response (route (create-offer-request test-editor draftset-location :invalid))]
    (assert-is-bad-request-response offer-response)))

(defn- create-claim-request [user draftset-location]
  (with-identity user {:uri (str draftset-location "/claim") :request-method :post}))

(deftest claim-draftset
  (let [draftset-location (create-draftset-through-api test-editor)]
    (offer-draftset-through-api test-editor draftset-location :publisher)

    (let [claim-request (create-claim-request test-publisher draftset-location)
          claim-response (route claim-request)]
      (assert-is-ok-response claim-response)

      (let [{:keys [current-owner]} (get-draftset-info-through-api draftset-location test-publisher)]
        (is (= (:email test-publisher) current-owner))))))

(deftest claim-draftset-owned-by-other-user
  (let [draftset-location (create-draftset-through-api test-editor)
        claim-request (create-claim-request test-publisher draftset-location)
        claim-response (route claim-request)]
    (assert-is-forbidden-response claim-response)))

(deftest claim-draftset-by-user-not-in-role
  (let [other-editor (user/create-user "edtheduck@example.com" :editor "quack")
        draftset-location (create-draftset-through-api test-editor)]
    (offer-draftset-through-api test-editor draftset-location :publisher)
    (let [claim-response (route (create-claim-request other-editor draftset-location))]
      (assert-is-forbidden-response claim-response))))

(deftest claim-non-existent-draftset
  (let [claim-response (route (create-claim-request test-publisher "/draftset/missing"))]
    (assert-is-not-found-response claim-response)))

(defn- setup-route [test-function]
  (binding [*route* (draftset-api-routes *test-backend*)]
    (test-function)))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each setup-route)
(use-fixtures :each (fn [tf]
                      (wrap-clean-test-db #(setup-route tf))))
