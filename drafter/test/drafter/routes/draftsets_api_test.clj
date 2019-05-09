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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Handler tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ok-response->typed-body [schema {:keys [body] :as response}]
  (tc/assert-is-ok-response response)
  (tc/assert-schema schema body)
  body)
