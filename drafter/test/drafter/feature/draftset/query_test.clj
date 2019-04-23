(ns drafter.feature.draftset.query-test
  (:require [clojure.test :as t :refer [is]]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]]
            [grafter-2.rdf.protocols :refer [add context ->Quad ->Triple map->Triple]]
            [swirrl-server.async.jobs :refer [finished-jobs]]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.util :as util])
  (:import org.eclipse.rdf4j.query.QueryResultHandler
           org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONParser))

(t/use-fixtures :each tc/with-spec-instrumentation)

(defn- result-set-handler [result-state]
  (reify QueryResultHandler
    (handleBoolean [this b])
    (handleLinks [this links])
    (startQueryResult [this binding-names])
    (endQueryResult [this])
    (handleSolution [this binding-set]
      (let [binding-pairs (map (fn [b] [(keyword (.getName b)) (.stringValue (.getValue b))]) binding-set)
            binding-map (into {} binding-pairs)]
        (swap! result-state conj binding-map)))))

(defn- create-query-request [user draftset-location query accept-content-type & {:keys [union-with-live?]}]
  (tc/with-identity user
    {:uri (str draftset-location "/query")
     :headers {"accept" accept-content-type}
     :request-method :post
     :params {:query query :union-with-live union-with-live?}}))

(defn- select-query-draftset-through-api [handler user draftset-location select-query & {:keys [union-with-live?]}]
  (let [request (create-query-request user draftset-location select-query "application/sparql-results+json" :union-with-live? union-with-live?)
        {:keys [body] :as query-response} (handler request)]
    (tc/assert-is-ok-response query-response)
    (let [result-state (atom #{})
          result-handler (result-set-handler result-state)
          parser (doto (SPARQLResultsJSONParser.) (.setQueryResultHandler result-handler))]

      (.parse parser body)
      @result-state)))

(tc/deftest-system-with-keys query-draftset-with-data
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        draftset-data-file "test/resources/test-draftset.trig"
        append-response (help/make-append-data-to-draftset-request handler test-editor draftset-location draftset-data-file)]
    (tc/await-success finished-jobs (:finished-job (:body append-response)) )
    (let [query "CONSTRUCT { ?s ?p ?o }  WHERE { GRAPH ?g { ?s ?p ?o } }"
          query-request (create-query-request test-editor draftset-location query "application/n-triples")
          query-response (handler query-request)
          response-triples (set (map #(util/map-values str %) (statements (:body query-response) :format :nt)) )
          expected-triples (set (map (comp #(util/map-values str %) map->Triple) (statements draftset-data-file)))]
      (tc/assert-is-ok-response query-response)

      (is (= expected-triples response-triples)))))

(tc/deftest-system-with-keys query-draftset-not-unioned-with-live-with-published-statements
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [grouped-quads (group-by context (statements "test/resources/test-draftset.trig"))
        [live-graph live-quads] (first grouped-quads)
        [ds-live-graph ds-quads] (second grouped-quads)
        draftset-location (help/create-draftset-through-api handler test-editor)]
    (help/publish-quads-through-api handler live-quads)
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location ds-quads)
    (let [q "SELECT * WHERE { GRAPH ?c { ?s ?p ?o } }"
          results (select-query-draftset-through-api handler test-editor draftset-location q :union-with-live? "false")
          expected-quads (help/eval-statements ds-quads)]
      (is (= (set expected-quads) (set results))))))

(tc/deftest-system-with-keys query-draftset-with-malformed-union-with-live
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        q "SELECT * WHERE { ?s ?p ?o }"
        request (create-query-request test-editor draftset-location q "application/sparql-results+json" :union-with-live? "notbool")
        response (handler request)]
    (tc/assert-is-unprocessable-response response)))

(tc/deftest-system-with-keys query-draftset-unioned-with-live
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api :drafter/write-scheduler]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [test-quads (statements "test/resources/test-draftset.trig")
        grouped-test-quads (group-by context test-quads)
        [live-graph live-quads] (first grouped-test-quads)
        [ds-live-graph draftset-quads] (second grouped-test-quads)
        draftset-location (help/create-draftset-through-api handler test-editor)]

    (help/publish-quads-through-api handler live-quads)
    (help/append-quads-to-draftset-through-api handler test-editor draftset-location draftset-quads)

    (let [query "SELECT * WHERE { GRAPH ?c { ?s ?p ?o } }"
          query-request (create-query-request test-editor draftset-location query "application/sparql-results+json" :union-with-live? "true")
          {:keys [body] :as query-response} (handler query-request)
          result-state (atom #{})
          result-handler (result-set-handler result-state)
          parser (doto (SPARQLResultsJSONParser.) (.setQueryResultHandler result-handler))]

      (.parse parser body)

      (let [expected-quads (set (help/eval-statements test-quads))]
        (is (= expected-quads @result-state))))))

(tc/deftest-system-with-keys query-non-existent-draftset
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [request (create-query-request test-editor "/v1/draftset/missing" "SELECT * WHERE { ?s ?p ?o }" "application/sparql-results+json")
        response (handler request)]
    (tc/assert-is-not-found-response response)))

(tc/deftest-system-with-keys query-draftset-request-with-missing-query-parameter
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        response (handler (tc/with-identity test-editor {:uri (str draftset-location "/query") :request-method :post}))]
    (tc/assert-is-unprocessable-response response)))

(tc/deftest-system-with-keys query-draftset-request-with-invalid-http-method
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        query-request (create-query-request test-editor draftset-location "SELECT * WHERE { ?s ?p ?o }" "text/plain")
        query-request (assoc query-request :request-method :put)
        response (handler query-request)]
    (tc/assert-is-method-not-allowed-response response)))

(tc/deftest-system-with-keys query-draftset-by-non-owner
  [:drafter.fixture-data/loader :drafter.routes/draftsets-api]
  [{handler :drafter.routes/draftsets-api} "test-system.edn"]
  (let [draftset-location (help/create-draftset-through-api handler test-editor)
        query-request (create-query-request test-publisher draftset-location "SELECT * WHERE { ?s ?p ?o }" "application/sparql-results+json")
        query-response (handler query-request)]
    (tc/assert-is-forbidden-response query-response)))
