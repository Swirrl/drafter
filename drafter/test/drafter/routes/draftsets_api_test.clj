(ns drafter.routes.draftsets-api-test
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :refer :all :as t]
            [drafter.middleware :as middleware]
            [drafter.rdf.drafter-ontology
             :refer
             [drafter:DraftGraph drafter:modifiedAt]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.feature.draftset.create-test :as create-test]
            [drafter.rdf.sparql :as sparql]
            [drafter.routes.draftsets-api :as sut :refer :all]
            [drafter.swagger :as swagger]
            [drafter.test-common :as tc]
            [drafter.timeouts :as timeouts]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-manager test-password test-publisher]]
            [drafter.user.memory-repository :as memrepo]
            [drafter.util :as util]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]]
            [grafter-2.rdf.protocols :refer [add context ->Quad ->Triple map->Triple]]
            [grafter-2.rdf4j.formats :as formats]
            [schema.core :as s]
            [swirrl-server.async.jobs :refer [finished-jobs]]
            [drafter.feature.draftset.test-helper :refer :all])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]
           java.net.URI
           java.time.OffsetDateTime
           org.eclipse.rdf4j.query.QueryResultHandler
           org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONParser))

(def ^:private ^:dynamic *route* nil)
(def ^:private ^:dynamic *user-repo* nil)

(defn- setup-route [test-function]
  (let [users (:drafter.user/memory-repository tc/*test-system*)
        swagger-spec (swagger/load-spec-and-resolve-refs)
        api-handler (:drafter.routes/draftsets-api tc/*test-system*)]

    (binding [*user-repo* users
              *route* (swagger/wrap-response-swagger-validation swagger-spec api-handler)]
      (test-function))))

(defn- route [request]
  (*route* request))

(use-fixtures :each (join-fixtures [(tc/wrap-system-setup "test-system.edn" [:drafter.user/repo :drafter.routes/draftsets-api :drafter.backend/rdf4j-repo :drafter/write-scheduler])
                                    setup-route])
  tc/with-spec-instrumentation)

(deftest get-all-draftsets-changes-test
  (let [grouped-quads (group-by context (statements "test/resources/test-draftset.trig"))
        [graph1 graph1-quads] (first grouped-quads)
        [graph2 graph2-quads] (second grouped-quads)
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route graph1-quads)

    ;;delete quads from graph1 and insert into graph2
    (delete-quads-through-api route test-editor draftset-location (take 1 graph1-quads))
    (append-quads-to-draftset-through-api route test-editor draftset-location graph2-quads)

    (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api route draftset-location test-editor)]
      (is (= :updated (get-in changes [graph1 :status])))
      (is (= :created (get-in changes [graph2 :status]))))

    ;;delete graph1
    (delete-draftset-graph-through-api route test-editor draftset-location graph1)
    (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api route draftset-location test-editor)]
      (is (= :deleted (get-in changes [graph1 :status])))
      (is (= :created (get-in changes [graph2 :status]))))))

(deftest get-empty-draftset-without-title-or-description
  (let [draftset-location (create-draftset-through-api route test-editor)
        ds-info (get-draftset-info-through-api route draftset-location test-editor)]
    (tc/assert-schema DraftsetWithoutTitleOrDescription ds-info)))

(deftest get-empty-draftset-without-description
  (let [display-name "Test title!"
        draftset-location (create-draftset-through-api route test-editor display-name)
        ds-info (get-draftset-info-through-api route draftset-location test-editor)]
    (tc/assert-schema DraftsetWithoutDescription ds-info)
    (is (= display-name (:display-name ds-info)))))

(deftest get-empty-draftset-with-description
  (let [display-name "Test title!"
        description "Draftset used in a test"
        draftset-location (create-draftset-through-api route test-editor display-name description)]

    (let [ds-info (get-draftset-info-through-api route draftset-location test-editor)]
      (tc/assert-schema draftset-with-description-info-schema ds-info)
      (is (= display-name (:display-name ds-info)))
      (is (= description (:description ds-info))))))

(deftest get-draftset-containing-data
  (let [display-name "Test title!"
        draftset-location (create-draftset-through-api route test-editor display-name)
        quads (statements "test/resources/test-draftset.trig")
        live-graphs (set (keys (group-by context quads)))]
    (append-quads-to-draftset-through-api route test-editor draftset-location quads)

    (let [ds-info (get-draftset-info-through-api route draftset-location test-editor)]
      (tc/assert-schema DraftsetWithoutDescription ds-info)

      (is (= display-name (:display-name ds-info)))
      (is (= live-graphs (tc/key-set (:changes ds-info)))))))

(deftest get-draftset-request-for-non-existent-draftset
  (let [response (route (get-draftset-info-request "/v1/draftset/missing" test-publisher))]
    (tc/assert-is-not-found-response response)))

(deftest get-draftset-available-for-claim
  (let [draftset-location (create-draftset-through-api route test-editor)]
    (submit-draftset-to-role-through-api route test-editor draftset-location :publisher)
    (let [ds-info (get-draftset-info-through-api route draftset-location test-publisher)])))

(deftest get-draftset-for-other-user-test
  (let [draftset-location (create-draftset-through-api route test-editor)
        get-request (get-draftset-info-request draftset-location test-publisher)
        get-response (route get-request)]
    (tc/assert-is-forbidden-response get-response)))

(deftest append-quad-data-with-valid-content-type-to-draftset
  (let [data-file-path "test/resources/test-draftset.trig"
        quads (statements data-file-path)
        draftset-location (create-draftset-through-api route test-editor)]
    (append-quads-to-draftset-through-api route test-editor draftset-location quads)
    (let [draftset-graphs (tc/key-set (:changes (get-draftset-info-through-api route draftset-location test-editor)))
          graph-statements (group-by context quads)]
      (doseq [[live-graph graph-quads] graph-statements]
        (let [graph-triples (get-draftset-graph-triples-through-api route draftset-location test-editor live-graph "false")
              expected-statements (map map->Triple graph-quads)]
          (is (contains? draftset-graphs live-graph))
          (is (set expected-statements) (set graph-triples)))))))

(deftest append-quad-data-to-graph-which-exists-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        live-quads (map (comp first second) grouped-quads)
        quads-to-add (rest (second (first grouped-quads)))
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route live-quads)
    (append-quads-to-draftset-through-api route test-editor draftset-location quads-to-add)

    ;;draftset itself should contain the live quads from the graph
    ;;added to along with the quads explicitly added. It should
    ;;not contain any quads from the other live graph.
    (let [draftset-quads (get-draftset-quads-through-api route draftset-location test-editor "false")
          expected-quads (eval-statements (second (first grouped-quads)))]
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest append-triple-data-to-draftset-test
  (with-open [fs (io/input-stream "test/test-triple.nt")]
    (let [draftset-location (create-draftset-through-api route test-editor)
          request (append-to-draftset-request test-editor draftset-location fs "application/n-triples")
          response (route request)]
      (is (is-client-error-response? response)))))

(deftest modifying-in-draftset-updates-modified-timestamp-test
  (let [quads (statements "test/resources/test-draftset.trig")
        draftset-location (create-draftset-through-api route test-editor)
        get-draft-graph-modified-at (fn []
                                      ;; There is only one draftgraph in this
                                      ;; test - so we can get away with a bit of
                                      ;; a sloppy query.
                                      (-> tc/*test-backend*
                                          (sparql/eager-query
                                           (str "SELECT ?modified {"
                                                "   ?draftgraph a <" drafter:DraftGraph "> ;"
                                                "                 <" drafter:modifiedAt ">   ?modified ."
                                                "}"))
                                          first
                                          (:modified)))]

    (testing "Publishing some triples sets the modified time"
      (append-triples-to-draftset-through-api route test-editor draftset-location quads "http://foo/")

      (let [first-timestamp (get-draft-graph-modified-at)]
        (is (instance? OffsetDateTime first-timestamp))

        (testing "Publishing more triples afterwards updates the modified time"

          (append-triples-to-draftset-through-api route test-editor draftset-location quads "http://foo/")
          (let [second-timestamp (get-draft-graph-modified-at)]
            (is (instance? OffsetDateTime second-timestamp))

            (is (.isBefore first-timestamp
                           second-timestamp)
                "Modified time is updated after append")

            (delete-triples-through-api route test-editor draftset-location quads "http://foo/")
            (let [third-timestamp (get-draft-graph-modified-at)]

              (is (.isBefore second-timestamp
                             third-timestamp)
                  "Modified time is updated after delete"))))))))

(deftest append-triples-to-graph-which-exists-in-live
  (let [[graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route [(first graph-quads)])
    (append-triples-to-draftset-through-api route test-editor draftset-location (rest graph-quads) graph)

    (let [draftset-graph-triples (get-draftset-graph-triples-through-api route draftset-location test-editor graph "false")
          expected-triples (eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(deftest append-quad-data-without-content-type-to-draftset
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api route test-editor)
          request (append-to-draftset-request test-editor draftset-location fs "tmp-content-type")
          request (update-in request [:headers] dissoc "content-type")
          response (route request)]
      (is (is-client-error-response? response)))))

(deftest append-data-to-non-existent-draftset
  (let [append-response (make-append-data-to-draftset-request route test-publisher "/v1/draftset/missing" "test/resources/test-draftset.trig")]
    (tc/assert-is-not-found-response append-response)))

(deftest append-quads-by-non-owner
  (let [draftset-location (create-draftset-through-api route test-editor)
        quads (statements "test/resources/test-draftset.trig")
        append-request (statements->append-request test-publisher draftset-location quads :nq)
        append-response (route append-request)]
    (tc/assert-is-forbidden-response append-response)))

(deftest append-graph-triples-by-non-owner
  (let [draftset-location (create-draftset-through-api route test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        append-request (statements->append-triples-request test-publisher draftset-location graph-quads graph)
        append-response (route append-request)]
    (tc/assert-is-forbidden-response append-response)))

(deftest delete-draftset-data-for-non-existent-draftset
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [delete-request (tc/with-identity test-manager {:uri "/v1/draftset/missing/data" :request-method :delete :body fs})
          delete-response (route delete-request)]
      (tc/assert-is-not-found-response delete-response))))

(deftest delete-draftset-data-request-with-unknown-content-type
  (with-open [input-stream (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api route test-editor)
          delete-request (create-delete-quads-request test-editor draftset-location input-stream "application/unknown-quads-format")
          delete-response (route delete-request)]
      (tc/assert-is-unsupported-media-type-response delete-response))))

(deftest delete-live-graph-not-in-draftset
  (let [quads (statements "test/resources/test-draftset.trig")
        graph-quads (group-by context quads)
        live-graphs (keys graph-quads)
        graph-to-delete (first live-graphs)
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route quads)
    (let [{draftset-graphs :changes} (delete-draftset-graph-through-api route test-editor draftset-location graph-to-delete)]
      (is (= #{graph-to-delete} (set (keys draftset-graphs)))))))

(deftest delete-graph-with-changes-in-draftset
  (let [draftset-location (create-draftset-through-api route test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        published-quad (first graph-quads)
        added-quads (rest graph-quads)]
    (publish-quads-through-api route [published-quad])
    (append-quads-to-draftset-through-api route test-editor draftset-location added-quads)

    (let [{draftset-graphs :changes} (delete-draftset-graph-through-api route test-editor draftset-location graph)]
      (is (= #{graph} (set (keys draftset-graphs)))))))

(deftest delete-graph-only-in-draftset
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api route test-editor)
        draftset-quads (statements rdf-data-file)
        grouped-quads (group-by context draftset-quads)
        [graph _] (first grouped-quads)]
    (append-data-to-draftset-through-api route test-editor draftset-location rdf-data-file)

    (let [{:keys [changes]} (delete-draftset-graph-through-api route test-editor draftset-location graph)
          draftset-graphs (keys changes)
          remaining-quads (eval-statements (get-draftset-quads-through-api route draftset-location test-editor))
          expected-quads (eval-statements (mapcat second (rest grouped-quads)))
          expected-graphs (keys grouped-quads)]
      (is (= (set expected-quads) (set remaining-quads)))
      (is (= (set expected-graphs) (set draftset-graphs))))))

(deftest delete-graph-request-for-non-existent-draftset
  (let [request (tc/with-identity test-manager {:uri "/v1/draftset/missing/graph" :request-method :delete :params {:graph "http://some-graph"}})
        response (route request)]
    (tc/assert-is-not-found-response response)))

(deftest publish-draftset-with-graphs-not-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        draftset-location (create-draftset-through-api route test-publisher)]
    (append-quads-to-draftset-through-api route test-publisher draftset-location quads)
    (publish-draftset-through-api route draftset-location test-publisher)

    (let [live-quads (get-live-quads-through-api route)]
      (is (= (set (eval-statements quads)) (set live-quads))))))

(deftest publish-draftset-with-statements-added-to-graphs-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (create-draftset-through-api route test-publisher)
        initial-live-quads (map (comp first second) grouped-quads)
        appended-quads (mapcat (comp rest second) grouped-quads)]

    (publish-quads-through-api route initial-live-quads)
    (append-quads-to-draftset-through-api route test-publisher draftset-location appended-quads)
    (publish-draftset-through-api route draftset-location test-publisher)

    (let [after-publish-quads (get-live-quads-through-api route)]
      (is (= (set (eval-statements quads)) (set after-publish-quads))))))

(deftest publish-draftset-with-statements-deleted-from-graphs-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (create-draftset-through-api route test-publisher)
        to-delete (map (comp first second) grouped-quads)]
    (publish-quads-through-api route quads)
    (delete-quads-through-api route test-publisher draftset-location to-delete)
    (publish-draftset-through-api route draftset-location test-publisher)

    (let [after-publish-quads (get-live-quads-through-api route)
          expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))]
      (is (= (set expected-quads) (set after-publish-quads))))))

(deftest publish-draftset-with-graph-deleted-from-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (create-draftset-through-api route test-publisher)
        graph-to-delete (ffirst grouped-quads)
        expected-quads (eval-statements (mapcat second (rest grouped-quads)))]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api route test-publisher draftset-location graph-to-delete)
    (publish-draftset-through-api route draftset-location test-publisher)
    (assert-live-quads route expected-quads)))

(deftest publish-draftset-with-deletes-and-appends-from-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph initial-quads] (first grouped-quads)
        draftset-location (create-draftset-through-api route test-publisher)
        to-add (take 2 (second (second grouped-quads)))
        to-delete (take 1 initial-quads)
        expected-quads (eval-statements (set/difference (set/union (set initial-quads) (set to-add)) (set to-delete)))]

    (publish-quads-through-api route initial-quads)
    (append-quads-to-draftset-through-api route test-publisher draftset-location to-add)
    (delete-quads-through-api route test-publisher draftset-location to-delete)
    (publish-draftset-through-api route draftset-location test-publisher)

    (assert-live-quads route expected-quads)))

(deftest publish-draftest-with-deletions-from-graphs-not-yet-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [graph graph-quads] (first grouped-quads)
        draftset-location (create-draftset-through-api route test-publisher)]

    ;;delete quads in draftset before they exist in live
    (delete-quads-through-api route test-publisher draftset-location graph-quads)

    ;;add to live then publish draftset
    (publish-quads-through-api route graph-quads)
    (publish-draftset-through-api route draftset-location test-publisher)

    ;;graph should still exist in live
    (assert-live-quads route graph-quads)))

(deftest publish-non-existent-draftset
  (let [response (route (tc/with-identity test-publisher {:uri "/v1/draftset/missing/publish" :request-method :post}))]
    (tc/assert-is-not-found-response response)))

(deftest publish-by-non-publisher-test
  (let [draftset-location (create-draftset-through-api route test-editor)]
    (append-quads-to-draftset-through-api route test-editor draftset-location (statements "test/resources/test-draftset.trig"))
    (let [publish-response (route (create-publish-request draftset-location test-editor))]
      (tc/assert-is-forbidden-response publish-response))))

(deftest publish-by-non-owner-test
  (let [draftset-location (create-draftset-through-api route test-publisher)
        quads (statements "test/resources/test-draftset.trig")]
    (append-quads-to-draftset-through-api route test-publisher draftset-location quads)
    (let [publish-request (create-publish-request draftset-location test-manager)
          publish-response (route publish-request)]
      (tc/assert-is-forbidden-response publish-response))))

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

(defn- select-query-draftset-through-api [user draftset-location select-query & {:keys [union-with-live?]}]
  (let [request (create-query-request user draftset-location select-query "application/sparql-results+json" :union-with-live? union-with-live?)
        {:keys [body] :as query-response} (route request)]
    (tc/assert-is-ok-response query-response)
    (let [result-state (atom #{})
          result-handler (result-set-handler result-state)
          parser (doto (SPARQLResultsJSONParser.) (.setQueryResultHandler result-handler))]

      (.parse parser body)
      @result-state)))

(deftest query-draftset-with-data
  (let [draftset-location (create-draftset-through-api route test-editor)
        draftset-data-file "test/resources/test-draftset.trig"
        append-response (make-append-data-to-draftset-request route test-editor draftset-location draftset-data-file)]
    (tc/await-success finished-jobs (:finished-job (:body append-response)) )
    (let [query "CONSTRUCT { ?s ?p ?o }  WHERE { GRAPH ?g { ?s ?p ?o } }"
          query-request (create-query-request test-editor draftset-location query "application/n-triples")
          query-response (route query-request)
          response-triples (set (map #(util/map-values str %) (statements (:body query-response) :format :nt)) )
          expected-triples (set (map (comp #(util/map-values str %) map->Triple) (statements draftset-data-file)))]
      (tc/assert-is-ok-response query-response)

      (is (= expected-triples response-triples)))))

(deftest query-draftset-not-unioned-with-live-with-published-statements
  (let [grouped-quads (group-by context (statements "test/resources/test-draftset.trig"))
        [live-graph live-quads] (first grouped-quads)
        [ds-live-graph ds-quads] (second grouped-quads)
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route live-quads)
    (append-quads-to-draftset-through-api route test-editor draftset-location ds-quads)
    (let [q "SELECT * WHERE { GRAPH ?c { ?s ?p ?o } }"
          results (select-query-draftset-through-api test-editor draftset-location q :union-with-live? "false")
          expected-quads (eval-statements ds-quads)]
      (is (= (set expected-quads) (set results))))))

(deftest query-draftset-with-malformed-union-with-live
  (let [draftset-location (create-draftset-through-api route test-editor)
        q "SELECT * WHERE { ?s ?p ?o }"
        request (create-query-request test-editor draftset-location q "application/sparql-results+json" :union-with-live? "notbool")
        response (route request)]
    (tc/assert-is-unprocessable-response response)))

(deftest query-draftset-unioned-with-live
  (let [test-quads (statements "test/resources/test-draftset.trig")
        grouped-test-quads (group-by context test-quads)
        [live-graph live-quads] (first grouped-test-quads)
        [ds-live-graph draftset-quads] (second grouped-test-quads)
        draftset-location (create-draftset-through-api route test-editor)]

    (publish-quads-through-api route live-quads)
    (append-quads-to-draftset-through-api route test-editor draftset-location draftset-quads)

    (let [query "SELECT * WHERE { GRAPH ?c { ?s ?p ?o } }"
          query-request (create-query-request test-editor draftset-location query "application/sparql-results+json" :union-with-live? "true")
          {:keys [body] :as query-response} (route query-request)
          result-state (atom #{})
          result-handler (result-set-handler result-state)
          parser (doto (SPARQLResultsJSONParser.) (.setQueryResultHandler result-handler))]

      (.parse parser body)

      (let [expected-quads (set (eval-statements test-quads))]
        (is (= expected-quads @result-state))))))

(deftest query-non-existent-draftset
  (let [request (create-query-request test-editor "/v1/draftset/missing" "SELECT * WHERE { ?s ?p ?o }" "application/sparql-results+json")
        response (route request)]
    (tc/assert-is-not-found-response response)))

(deftest query-draftset-request-with-missing-query-parameter
  (let [draftset-location (create-draftset-through-api route test-editor)
        response (route (tc/with-identity test-editor {:uri (str draftset-location "/query") :request-method :post}))]
    (tc/assert-is-unprocessable-response response)))

(deftest query-draftset-request-with-invalid-http-method
  (let [draftset-location (create-draftset-through-api route test-editor)
        query-request (create-query-request test-editor draftset-location "SELECT * WHERE { ?s ?p ?o }" "text/plain")
        query-request (assoc query-request :request-method :put)
        response (route query-request)]
    (tc/assert-is-method-not-allowed-response response)))

(deftest query-draftset-by-non-owner
  (let [draftset-location (create-draftset-through-api route test-editor)
        query-request (create-query-request test-publisher draftset-location "SELECT * WHERE { ?s ?p ?o }" "application/sparql-results+json")
        query-response (route query-request)]
    (tc/assert-is-forbidden-response query-response)))

(deftest get-draftset-graph-triples-data
  (let [draftset-location (create-draftset-through-api route test-editor)
        draftset-data-file "test/resources/test-draftset.trig"
        input-quads (statements draftset-data-file)]
    (append-quads-to-draftset-through-api route test-editor draftset-location input-quads)

    (doseq [[graph quads] (group-by context input-quads)]
      (let [graph-triples (set (eval-statements (map map->Triple quads)))
            response-triples (set (get-draftset-graph-triples-through-api route draftset-location test-editor graph "false"))]
        (is (= graph-triples response-triples))))))

(deftest get-draftset-quads-data
  (let [draftset-location (create-draftset-through-api route test-editor)
        draftset-data-file "test/resources/test-draftset.trig"]
    (append-data-to-draftset-through-api route test-editor draftset-location draftset-data-file)

    (let [response-quads (set (get-draftset-quads-through-api route draftset-location test-editor))
          input-quads (set (eval-statements (statements draftset-data-file)))]
      (is (= input-quads response-quads)))))

(deftest get-draftset-quads-data-with-invalid-accept
  (let [draftset-location (create-draftset-through-api route test-editor)]
    (append-data-to-draftset-through-api route test-editor draftset-location "test/resources/test-draftset.trig")
    (let [data-request (get-draftset-quads-accept-request draftset-location test-editor "text/invalidrdfformat" "false")
          data-response (route data-request)]
      (tc/assert-is-not-acceptable-response data-response))))

(deftest get-draftset-quads-data-with-multiple-accepted
  (let [draftset-location (create-draftset-through-api route test-editor)]
    (append-data-to-draftset-through-api route test-editor draftset-location "test/resources/test-draftset.trig")
    (let [accepted "application/n-quads,application/trig,apllication/trix,application/n-triples,application/rdf+xml,text/turtle"
          data-request (get-draftset-quads-accept-request draftset-location test-editor accepted "false")
          data-response (route data-request)]
      (tc/assert-is-ok-response data-response))))

(deftest get-draftset-quads-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (first (keys grouped-quads))
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api route test-editor draftset-location graph-to-delete)

    (let [response-quads (set (get-draftset-quads-through-api route draftset-location test-editor "true"))
          expected-quads (set (eval-statements (mapcat second (rest grouped-quads))))]
      (is (= expected-quads response-quads)))))

(deftest get-added-draftset-quads-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph live-quads] (first grouped-quads)
        [draftset-graph draftset-quads] (second grouped-quads)
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route live-quads)
    (append-quads-to-draftset-through-api route test-editor draftset-location draftset-quads)

    (let [response-quads (set (get-draftset-quads-through-api route draftset-location test-editor "true"))
          expected-quads (set (eval-statements (concat live-quads draftset-quads)))]
      (is (= expected-quads response-quads)))))

(deftest get-draftset-triples-for-deleted-graph-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (ffirst grouped-quads)
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api route test-editor draftset-location graph-to-delete)

    (let [draftset-triples (get-draftset-graph-triples-through-api route draftset-location test-editor graph-to-delete "true")]
      (is (empty? draftset-triples)))))

(deftest get-draftset-triples-for-published-graph-not-in-draftset-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        [graph graph-quads] (first (group-by context quads))
        draftset-location (create-draftset-through-api route)]
    (publish-quads-through-api route graph-quads)

    (let [draftset-graph-triples (get-draftset-graph-triples-through-api route draftset-location test-editor graph "true")
          expected-triples (eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(deftest get-draftset-graph-triples-request-without-graph
  (let [draftset-location (create-draftset-through-api route test-editor)]
    (append-quads-to-draftset-through-api route test-editor draftset-location (statements "test/resources/test-draftset.trig"))
    (let [data-request {:uri (str draftset-location "/data")
                        :request-method :get
                        :headers {"accept" "application/n-triples"}}
          data-request (tc/with-identity test-editor data-request)
          data-response (route data-request)]
      (tc/assert-is-not-acceptable-response data-response))))

(deftest get-draftset-data-for-missing-draftset
  (let [response (route (tc/with-identity test-manager {:uri "/v1/draftset/missing/data" :request-method :get :headers {"accept" "application/n-quads"}}))]
    (tc/assert-is-not-found-response response)))

(deftest get-draftset-data-for-unowned-draftset
  (let [draftset-location (create-draftset-through-api route test-editor)
        get-data-request (get-draftset-quads-request draftset-location test-publisher :nq "false")
        response (route get-data-request)]
    (tc/assert-is-forbidden-response response)))

(defn- create-update-draftset-metadata-request [user draftset-location title description]
  (tc/with-identity user
    {:uri draftset-location :request-method :put :params {:display-name title :description description}}))

(defn- update-draftset-metadata-through-api [user draftset-location title description]
  (let [request (create-update-draftset-metadata-request user draftset-location title description)
        {:keys [body] :as response} (route request)]
    (tc/assert-is-ok-response response)
    (tc/assert-schema Draftset body)
    body))

(deftest set-draftset-with-existing-title-and-description-metadata
  (let [draftset-location (create-draftset-through-api route test-editor "Test draftset" "Test description")
        new-title "Updated title"
        new-description "Updated description"
        {:keys [display-name description]} (update-draftset-metadata-through-api test-editor draftset-location new-title new-description)]
    (is (= new-title display-name))
    (is (= new-description description))))

(deftest set-metadata-for-draftset-with-no-title-or-description
  (let [draftset-location (create-draftset-through-api route)
        new-title "New title"
        new-description "New description"
        {:keys [display-name description]} (update-draftset-metadata-through-api test-editor draftset-location new-title new-description)]
    (is (= new-title display-name))
    (is (= new-description description))))

(deftest set-missing-draftset-metadata
  (let [meta-request (create-update-draftset-metadata-request test-manager "/v1/draftset/missing" "Title!" "Description")
        meta-response (route meta-request)]
    (tc/assert-is-not-found-response meta-response)))

(deftest set-metadata-by-non-owner
  (let [draftset-location (create-draftset-through-api route test-editor "Test draftset" "Test description")
        update-request (create-update-draftset-metadata-request test-publisher draftset-location "New title" "New description")
        update-response (route update-request)]
    (tc/assert-is-forbidden-response update-response)))

(deftest submit-draftset-to-role
  (let [draftset-location (create-draftset-through-api route test-editor)
        submit-request (create-submit-to-role-request test-editor draftset-location :publisher)
        {ds-info :body :as submit-response} (route submit-request)]
    (tc/assert-is-ok-response submit-response)
    (tc/assert-schema Draftset ds-info)

    (is (= false (contains? ds-info :current-owner))))
  )

(deftest get-options-test
  (let [draftset-location (create-draftset-through-api route test-editor)
        options-request (tc/with-identity test-editor {:uri draftset-location :request-method :options})
        {:keys [body] :as options-response} (route options-request)]
    (tc/assert-is-ok-response options-response)
    (is (= #{:edit :delete :submit :claim} (set body)))))

(deftest get-options-for-non-existent-draftset
  (let [response (route (tc/with-identity test-manager {:uri "/v1/draftset/missing" :request-method :options}))]
    (tc/assert-is-not-found-response response)))

(defn- revert-draftset-graph-changes-request [draftset-location user graph]
  (tc/with-identity user {:uri (str draftset-location "/changes") :request-method :delete :params {:graph (str graph)}}))

(defn- revert-draftset-graph-changes-through-api [draftset-location user graph]
  (let [{:keys [body] :as response} (route (revert-draftset-graph-changes-request draftset-location user graph))]
    (tc/assert-is-ok-response response)
    (tc/assert-schema Draftset body)
    body))

(deftest revert-graph-change-in-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api route test-editor draftset-location live-graph)

    (let [{:keys [changes]} (get-draftset-info-through-api route draftset-location test-editor)]
      (is (= #{live-graph} (tc/key-set changes))))

    (let [{:keys [changes] :as ds-info} (revert-draftset-graph-changes-through-api draftset-location test-editor live-graph)]
      (is (= #{} (tc/key-set changes))))

    (let [ds-quads (get-draftset-quads-through-api route draftset-location test-editor "true")]
      (is (= (set (eval-statements quads)) (set ds-quads))))))

(deftest revert-graph-change-in-unowned-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api route test-editor draftset-location live-graph)

    (let [revert-request (revert-draftset-graph-changes-request draftset-location test-publisher live-graph)
          response (route revert-request)]
      (tc/assert-is-forbidden-response response))))

(deftest revert-graph-change-in-draftset-unauthorised
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route quads)
    (delete-draftset-graph-through-api route test-editor draftset-location live-graph)

    (let [revert-request {:uri (str draftset-location "/changes") :request-method :delete :params {:graph live-graph}}
          response (route revert-request)]
      (tc/assert-is-unauthorised-response response))))

(deftest revert-non-existent-graph-change-in-draftest
  (let [draftset-location (create-draftset-through-api route test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "http://missing")
        response (route revert-request)]
    (tc/assert-is-not-found-response response)))

(deftest revert-change-in-non-existent-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (publish-quads-through-api route quads)
    (let [revert-request (revert-draftset-graph-changes-request "/v1/draftset/missing" test-manager live-graph)
          response (route revert-request)]
      (tc/assert-is-not-found-response response))))

(deftest revert-graph-change-request-without-graph-parameter
  (let [draftset-location (create-draftset-through-api route test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "tmp")
        revert-request (update-in revert-request [:params] dissoc :graph)
        response (route revert-request)]
    (tc/assert-is-unprocessable-response response)))

(defn- copy-live-graph-into-draftset-request [draftset-location user live-graph]
  (tc/with-identity
    user
    {:uri (str draftset-location "/graph") :request-method :put :params {:graph (str live-graph)}}))

(defn- copy-live-graph-into-draftset [draftset-location user live-graph]
  (let [request (copy-live-graph-into-draftset-request draftset-location user live-graph)
        response (route request)]
    (tc/await-success finished-jobs (:finished-job (:body response)))))

(deftest copy-live-graph-into-draftset-test
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route quads)
    (copy-live-graph-into-draftset draftset-location test-editor live-graph)

    (let [ds-quads (get-draftset-quads-through-api route draftset-location test-editor "false")
          expected-quads (eval-statements quads)]
      (is (= (set expected-quads) (set ds-quads))))))

(deftest copy-live-graph-with-existing-draft-into-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        [published private] (split-at 2 quads)
        draftset-location (create-draftset-through-api route test-editor)]

    ;;publish some graph quads to live and some others into the draftset
    (publish-quads-through-api route published)
    (append-quads-to-draftset-through-api route test-editor draftset-location private)

    ;;copy live graph into draftset
    (copy-live-graph-into-draftset draftset-location test-editor live-graph)

    ;;draftset graph should contain only the publish quads
    (let [graph-quads (get-draftset-quads-through-api route draftset-location test-editor "false")]
      (is (= (set (eval-statements published)) (set graph-quads))))))

(deftest copy-live-graph-into-unowned-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api route test-editor)]
    (publish-quads-through-api route quads)
    (let [copy-request (copy-live-graph-into-draftset-request draftset-location test-publisher live-graph)
          copy-response (route copy-request)]
      (tc/assert-is-forbidden-response copy-response))))

(deftest copy-non-existent-live-graph
  (let [draftset-location (create-draftset-through-api route test-editor)
        copy-request (copy-live-graph-into-draftset-request draftset-location test-editor "http://missing")
        copy-response (route copy-request)]
    (tc/assert-is-unprocessable-response copy-response)))

(deftest copy-live-graph-into-non-existent-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (publish-quads-through-api route quads)
    (let [copy-request (copy-live-graph-into-draftset-request "/v1/draftset/missing" test-publisher live-graph)
          copy-response (route copy-request)]
      (tc/assert-is-not-found-response copy-response))))

(deftest draftset-graphs-state-test
  (testing "Graph created"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (create-draftset-through-api route test-editor)]
      (append-quads-to-draftset-through-api route test-editor draftset-location quads)
      (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api route draftset-location test-editor)]
        (is (= :created (get-in changes [live-graph :status]))))))

  (testing "Quads deleted from live graph"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (create-draftset-through-api route test-editor)]
      (publish-quads-through-api route quads)
      (delete-quads-through-api route test-editor draftset-location (take 1 quads))

      (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api route draftset-location test-editor)]
        (is (= :updated (get-in changes [live-graph :status]))))))

  (testing "Quads added to live graph"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          [published to-add] (split-at 1 quads)
          draftset-location (create-draftset-through-api route test-editor)]
      (publish-quads-through-api route published)
      (append-quads-to-draftset-through-api route test-editor draftset-location to-add)

      (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api route draftset-location test-editor)]
        (is (= :updated (get-in changes [live-graph :status]))))))

  (testing "Graph deleted"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (create-draftset-through-api route test-editor)]
      (publish-quads-through-api route quads)
      (delete-draftset-graph-through-api route test-editor draftset-location live-graph)

      (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api route draftset-location test-editor)]
        (is (= :deleted (get-in changes [live-graph :status])))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Handler tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ok-response->typed-body [schema {:keys [body] :as response}]
  (tc/assert-is-ok-response response)
  (tc/assert-schema schema body)
  body)
