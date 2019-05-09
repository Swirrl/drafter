(ns drafter.routes.draftsets-api-test
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.test :as t :refer :all]
            [drafter.feature.draftset.test-helper :refer :all]
            [drafter.rdf.drafter-ontology
             :refer
             [drafter:DraftGraph drafter:modifiedAt]]
            [drafter.rdf.draftset-management.job-util :as jobs]
            [drafter.rdf.sparql :as sparql]
            [drafter.swagger :as swagger]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user-test
             :refer
             [test-editor test-manager test-password test-publisher]]
            [drafter.user.memory-repository :as memrepo]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :refer [->Quad ->Triple context map->Triple]]
            [grafter-2.rdf4j.formats :as formats]
            [grafter-2.rdf4j.io :refer [statements]]
            [swirrl-server.async.jobs :refer [finished-jobs]])
  (:import java.net.URI
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


(deftest submit-draftset-to-role
  (let [draftset-location (create-draftset-through-api route test-editor)
        submit-request (create-submit-to-role-request test-editor draftset-location :publisher)
        {ds-info :body :as submit-response} (route submit-request)]
    (tc/assert-is-ok-response submit-response)
    (tc/assert-schema Draftset ds-info)

    (is (= false (contains? ds-info :current-owner))))
  )


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Handler tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ok-response->typed-body [schema {:keys [body] :as response}]
  (tc/assert-is-ok-response response)
  (tc/assert-schema schema body)
  body)
