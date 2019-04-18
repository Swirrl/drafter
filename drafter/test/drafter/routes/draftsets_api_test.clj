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
            [swirrl-server.async.jobs :refer [finished-jobs]])
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

(use-fixtures :each (join-fixtures [(tc/wrap-system-setup "test-system.edn" [:drafter.user/repo :drafter.routes/draftsets-api :drafter.backend/rdf4j-repo :drafter/write-scheduler])
                                    setup-route])
  tc/with-spec-instrumentation)


(defn is-client-error-response?
  "Whether the given ring response map represents a client error."
  [{:keys [status] :as response}]
  (and (>= status 400)
       (< status 500)))

(defn- route [request]
  (*route* request))

(defn- statements->input-stream [statements format]
  (let [bos (ByteArrayOutputStream.)
        serialiser (rdf-writer bos :format format)]
    (add serialiser statements)
    (ByteArrayInputStream. (.toByteArray bos))))

(defn- append-to-draftset-request [user draftset-location data-stream content-type]
  (tc/with-identity user
    {:uri (str draftset-location "/data")
     :request-method :put
     :body data-stream
     :headers {"content-type" content-type}}))

(defn- make-append-data-to-draftset-request [user draftset-location data-file-path]
  (with-open [fs (io/input-stream data-file-path)]
    (let [request (append-to-draftset-request user draftset-location fs "application/x-trig")]
      (route request))))

(defn- append-data-to-draftset-through-api [user draftset-location draftset-data-file]
  (let [append-response (make-append-data-to-draftset-request user draftset-location draftset-data-file)]
    (tc/await-success finished-jobs (:finished-job (:body append-response)))))

(defn- statements->append-request [user draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)]
    (append-to-draftset-request user draftset-location input-stream (.getDefaultMIMEType (formats/->rdf-format format)))))

(defn- append-quads-to-draftset-through-api [user draftset-location quads]
  (let [request (statements->append-request user draftset-location quads :nq)
        response (route request)]
    (tc/await-success finished-jobs (get-in response [:body :finished-job]))))

(defn- statements->append-triples-request [user draftset-location triples graph]
  (-> (statements->append-request user draftset-location triples :nt)
      (assoc-in [:params :graph] (str graph))))

(defn- append-triples-to-draftset-through-api [user draftset-location triples graph]
  (let [request (statements->append-triples-request user draftset-location triples graph)
        response (route request)]
    (tc/await-success finished-jobs (get-in response [:body :finished-job]))))

(def ^:private DraftsetWithoutTitleOrDescription
  {:id s/Str
   :changes {URI {:status (s/enum :created :updated :deleted)}}
   :created-at OffsetDateTime
   :updated-at OffsetDateTime
   :created-by s/Str
   (s/optional-key :current-owner) s/Str
   (s/optional-key :claim-role) s/Keyword
   (s/optional-key :claim-user) s/Str
   (s/optional-key :submitted-by) s/Str})

(def ^:private DraftsetWithoutDescription
  (assoc DraftsetWithoutTitleOrDescription :display-name s/Str))

(def ^:private draftset-with-description-info-schema
  (assoc DraftsetWithoutDescription :description s/Str))

(def Draftset
  (merge DraftsetWithoutTitleOrDescription
         {(s/optional-key :description) s/Str
          (s/optional-key :display-name) s/Str}))

(defn- eval-statement [s]
  (util/map-values str s))

(defn- eval-statements [ss]
  (map eval-statement ss))

(defn- concrete-statements [source format]
  (eval-statements (statements source :format format)))

(defn create-draftset-through-api
  ([] (create-draftset-through-api test-editor))
  ([user] (create-draftset-through-api user nil))
  ([user display-name] (create-draftset-through-api user display-name nil))
  ([user display-name description]
   (let [request (create-test/create-draftset-request user display-name description)
         {:keys [headers] :as response} (route request)]
     (create-test/assert-is-see-other-response response)
     (get headers "Location"))))

(defn- get-draftset-quads-accept-request [draftset-location user accept union-with-live?-str]
  (tc/with-identity user
    {:uri (str draftset-location "/data")
     :request-method :get
     :headers {"accept" accept}
     :params {:union-with-live union-with-live?-str}}))

(defn- get-draftset-quads-request [draftset-location user format union-with-live?-str]
  (get-draftset-quads-accept-request draftset-location user (.getDefaultMIMEType (formats/->rdf-format format)) union-with-live?-str))

(defn- get-draftset-quads-through-api
  ([draftset-location user]
   (get-draftset-quads-through-api draftset-location user "false"))
  ([draftset-location user union-with-live?]
   (let [data-request (get-draftset-quads-request draftset-location user :nq union-with-live?)
         data-response (route data-request)]
     (tc/assert-is-ok-response data-response)
     (concrete-statements (:body data-response) :nq))))

(defn- get-draftset-graph-triples-through-api [draftset-location user graph union-with-live?-str]
  (let [data-request {:uri            (str draftset-location "/data")
                      :request-method :get
                      :headers        {"accept" "application/n-triples"}
                      :params         {:union-with-live union-with-live?-str :graph (str graph)}}
        data-request (tc/with-identity user data-request)
        {:keys [body] :as data-response} (route data-request)]
    (tc/assert-is-ok-response data-response)
    (concrete-statements body :nt)))

(defn- create-publish-request [draftset-location user]
  (tc/with-identity user {:uri (str draftset-location "/publish") :request-method :post}))

(defn- publish-draftset-through-api [draftset-location user]
  (let [publish-request (create-publish-request draftset-location user)
        publish-response (route publish-request)]
    (tc/await-success finished-jobs (:finished-job (:body publish-response)))))

(defn- publish-quads-through-api [quads]
  (let [draftset-location (create-draftset-through-api test-publisher)]
    (append-quads-to-draftset-through-api test-publisher draftset-location quads)
    (publish-draftset-through-api draftset-location test-publisher)))

(defn- create-delete-quads-request [user draftset-location input-stream format]
  (tc/with-identity user {:uri (str draftset-location "/data")
                       :request-method :delete
                       :body input-stream
                       :headers {"content-type" format}}))

(defn- get-draftset-info-request [draftset-location user]
  (tc/with-identity user {:uri draftset-location :request-method :get}))

(defn- get-draftset-info-through-api [draftset-location user]
  (let [{:keys [body] :as response} (route (get-draftset-info-request draftset-location user))]
    (tc/assert-is-ok-response response)
    (tc/assert-schema Draftset body)
    body))

(defn- delete-draftset-graph-request [user draftset-location graph-to-delete]
  (tc/with-identity user {:uri (str draftset-location "/graph") :request-method :delete :params {:graph (str graph-to-delete)}}))

(defn- delete-draftset-graph-through-api [user draftset-location graph-to-delete]
  (let [delete-graph-request (delete-draftset-graph-request user draftset-location graph-to-delete)
        {:keys [body] :as delete-graph-response} (route delete-graph-request)]
    (tc/assert-is-ok-response delete-graph-response)
    (tc/assert-schema Draftset body)
    body))

(defn- await-delete-statements-response [response]
  (let [job-result (tc/await-success finished-jobs (get-in response [:body :finished-job]))]
    (get-in job-result [:details :draftset])))

(defn- create-delete-statements-request [user draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)]
    (create-delete-quads-request user draftset-location input-stream (.getDefaultMIMEType (formats/->rdf-format format)))))

(defn- create-delete-triples-request [user draftset-location statements graph]
  (assoc-in (create-delete-statements-request user draftset-location statements :nt)
            [:params :graph] (str graph)))

(defn- delete-triples-through-api [user draftset-location triples graph]
  (-> (create-delete-triples-request user draftset-location triples graph)
      route
      await-delete-statements-response))

(defn- delete-quads-through-api [user draftset-location quads]
  (let [delete-request (create-delete-statements-request user draftset-location quads :nq)
        delete-response (route delete-request)]
    (await-delete-statements-response delete-response)))

(defn- delete-draftset-triples-through-api [user draftset-location triples graph]
  (let [delete-request (create-delete-statements-request user draftset-location triples :nt)
        delete-request (assoc-in delete-request [:params :graph] (str graph))
        delete-response (route delete-request)]
    (await-delete-statements-response delete-response)))

;;TODO: Get quads through query of live endpoint? This depends on
;;'union with live' working correctly
(defn- get-live-quads-through-api []
  (let [tmp-ds (create-draftset-through-api test-editor)]
    (get-draftset-quads-through-api tmp-ds test-editor "true")))

(defn- assert-live-quads [expected-quads]
  (let [live-quads (get-live-quads-through-api)]
    (is (= (set (eval-statements expected-quads)) (set live-quads)))))

(defn- create-submit-to-role-request [user draftset-location role]
  (tc/with-identity user {:uri (str draftset-location "/submit-to") :request-method :post :params {:role (name role)}}))

(defn- submit-draftset-to-role-through-api [user draftset-location role]
  (let [response (route (create-submit-to-role-request user draftset-location role))]
    (tc/assert-is-ok-response response)))

;; define a local alternative to the route fixture wrapper




#_(defn get-draftsets-request [include user]
  (tc/with-identity user
    {:uri "/v1/draftsets" :request-method :get :params {:include include}}))

#_(defn- get-draftsets-through-api [include user]
  (let [request (get-draftsets-request include user)
        {:keys [body] :as response} (route request)]
    (ok-response->typed-body [Draftset] response)))

#_(defn- get-all-draftsets-through-api [user]
  (get-draftsets-through-api :all user))

(defn- submit-draftset-to-username-request [draftset-location target-username user]
  (tc/with-identity user
    {:uri (str draftset-location "/submit-to") :request-method :post :params {:user target-username}}))

(defn- submit-draftset-to-user-request [draftset-location target-user user]
  (submit-draftset-to-username-request draftset-location (user/username target-user) user))

(defn- submit-draftset-to-user-through-api [draftset-location target-user user]
  (let [request (submit-draftset-to-user-request draftset-location target-user user)
        response (route request)]
    (tc/assert-is-ok-response response)))

(deftest get-all-draftsets-changes-test
  (let [grouped-quads (group-by context (statements "test/resources/test-draftset.trig"))
        [graph1 graph1-quads] (first grouped-quads)
        [graph2 graph2-quads] (second grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api graph1-quads)

    ;;delete quads from graph1 and insert into graph2
    (delete-quads-through-api test-editor draftset-location (take 1 graph1-quads))
    (append-quads-to-draftset-through-api test-editor draftset-location graph2-quads)

    (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api draftset-location test-editor)]
      (is (= :updated (get-in changes [graph1 :status])))
      (is (= :created (get-in changes [graph2 :status]))))

    ;;delete graph1
    (delete-draftset-graph-through-api test-editor draftset-location graph1)
    (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api draftset-location test-editor)]
      (is (= :deleted (get-in changes [graph1 :status])))
      (is (= :created (get-in changes [graph2 :status]))))))

(deftest get-empty-draftset-without-title-or-description
  (let [draftset-location (create-draftset-through-api test-editor)
        ds-info (get-draftset-info-through-api draftset-location test-editor)]
    (tc/assert-schema DraftsetWithoutTitleOrDescription ds-info)))

(deftest get-empty-draftset-without-description
  (let [display-name "Test title!"
        draftset-location (create-draftset-through-api test-editor display-name)
        ds-info (get-draftset-info-through-api draftset-location test-editor)]
    (tc/assert-schema DraftsetWithoutDescription ds-info)
    (is (= display-name (:display-name ds-info)))))

(deftest get-empty-draftset-with-description
  (let [display-name "Test title!"
        description "Draftset used in a test"
        draftset-location (create-draftset-through-api test-editor display-name description)]

    (let [ds-info (get-draftset-info-through-api draftset-location test-editor)]
      (tc/assert-schema draftset-with-description-info-schema ds-info)
      (is (= display-name (:display-name ds-info)))
      (is (= description (:description ds-info))))))

(deftest get-draftset-containing-data
  (let [display-name "Test title!"
        draftset-location (create-draftset-through-api test-editor display-name)
        quads (statements "test/resources/test-draftset.trig")
        live-graphs (set (keys (group-by context quads)))]
    (append-quads-to-draftset-through-api test-editor draftset-location quads)

    (let [ds-info (get-draftset-info-through-api draftset-location test-editor)]
      (tc/assert-schema DraftsetWithoutDescription ds-info)

      (is (= display-name (:display-name ds-info)))
      (is (= live-graphs (tc/key-set (:changes ds-info)))))))

(deftest get-draftset-request-for-non-existent-draftset
  (let [response (route (get-draftset-info-request "/v1/draftset/missing" test-publisher))]
    (tc/assert-is-not-found-response response)))

(deftest get-draftset-available-for-claim
  (let [draftset-location (create-draftset-through-api test-editor)]
    (submit-draftset-to-role-through-api test-editor draftset-location :publisher)
    (let [ds-info (get-draftset-info-through-api draftset-location test-publisher)])))

(deftest get-draftset-for-other-user-test
  (let [draftset-location (create-draftset-through-api test-editor)
        get-request (get-draftset-info-request draftset-location test-publisher)
        get-response (route get-request)]
    (tc/assert-is-forbidden-response get-response)))

(deftest append-quad-data-with-valid-content-type-to-draftset
  (let [data-file-path "test/resources/test-draftset.trig"
        quads (statements data-file-path)
        draftset-location (create-draftset-through-api test-editor)]
    (append-quads-to-draftset-through-api test-editor draftset-location quads)
    (let [draftset-graphs (tc/key-set (:changes (get-draftset-info-through-api draftset-location test-editor)))
          graph-statements (group-by context quads)]
      (doseq [[live-graph graph-quads] graph-statements]
        (let [graph-triples (get-draftset-graph-triples-through-api draftset-location test-editor live-graph "false")
              expected-statements (map map->Triple graph-quads)]
          (is (contains? draftset-graphs live-graph))
          (is (set expected-statements) (set graph-triples)))))))

(deftest append-quad-data-to-graph-which-exists-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        live-quads (map (comp first second) grouped-quads)
        quads-to-add (rest (second (first grouped-quads)))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api live-quads)
    (append-quads-to-draftset-through-api test-editor draftset-location quads-to-add)

    ;;draftset itself should contain the live quads from the graph
    ;;added to along with the quads explicitly added. It should
    ;;not contain any quads from the other live graph.
    (let [draftset-quads (get-draftset-quads-through-api draftset-location test-editor "false")
          expected-quads (eval-statements (second (first grouped-quads)))]
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest append-triple-data-to-draftset-test
  (with-open [fs (io/input-stream "test/test-triple.nt")]
    (let [draftset-location (create-draftset-through-api test-editor)
          request (append-to-draftset-request test-editor draftset-location fs "application/n-triples")
          response (route request)]
      (is (is-client-error-response? response)))))

(deftest modifying-in-draftset-updates-modified-timestamp-test
  (let [quads (statements "test/resources/test-draftset.trig")
        draftset-location (create-draftset-through-api test-editor)
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
      (append-triples-to-draftset-through-api test-editor draftset-location quads "http://foo/")

      (let [first-timestamp (get-draft-graph-modified-at)]
        (is (instance? OffsetDateTime first-timestamp))

        (testing "Publishing more triples afterwards updates the modified time"

          (append-triples-to-draftset-through-api test-editor draftset-location quads "http://foo/")
          (let [second-timestamp (get-draft-graph-modified-at)]
            (is (instance? OffsetDateTime second-timestamp))

            (is (.isBefore first-timestamp
                           second-timestamp)
                "Modified time is updated after append")

            (delete-triples-through-api test-editor draftset-location quads "http://foo/")
            (let [third-timestamp (get-draft-graph-modified-at)]

              (is (.isBefore second-timestamp
                             third-timestamp)
                  "Modified time is updated after delete"))))))))

(deftest append-triples-to-graph-which-exists-in-live
  (let [[graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api [(first graph-quads)])
    (append-triples-to-draftset-through-api test-editor draftset-location (rest graph-quads) graph)

    (let [draftset-graph-triples (get-draftset-graph-triples-through-api draftset-location test-editor graph "false")
          expected-triples (eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(deftest append-quad-data-without-content-type-to-draftset
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api test-editor)
          request (append-to-draftset-request test-editor draftset-location fs "tmp-content-type")
          request (update-in request [:headers] dissoc "content-type")
          response (route request)]
      (is (is-client-error-response? response)))))

(deftest append-data-to-non-existent-draftset
  (let [append-response (make-append-data-to-draftset-request test-publisher "/v1/draftset/missing" "test/resources/test-draftset.trig")]
    (tc/assert-is-not-found-response append-response)))

(deftest append-quads-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        quads (statements "test/resources/test-draftset.trig")
        append-request (statements->append-request test-publisher draftset-location quads :nq)
        append-response (route append-request)]
    (tc/assert-is-forbidden-response append-response)))

(deftest append-graph-triples-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        append-request (statements->append-triples-request test-publisher draftset-location graph-quads graph)
        append-response (route append-request)]
    (tc/assert-is-forbidden-response append-response)))

(deftest delete-quads-from-live-graphs-in-draftset
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        to-delete (map (comp first second) grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)

    (let [{graph-info :changes :as draftset-info} (delete-quads-through-api test-editor draftset-location to-delete)
          ds-graphs (keys graph-info)
          expected-graphs (map first grouped-quads)
          draftset-quads (get-draftset-quads-through-api draftset-location test-editor "false")
          expected-quads (eval-statements (mapcat (comp rest second) grouped-quads))]
      (is (= (set expected-graphs) (set ds-graphs)))
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest delete-quads-from-graph-not-in-live
  (let [draftset-location (create-draftset-through-api test-editor)
        to-delete [(->Quad (URI. "http://s1") (URI. "http://p1") (URI. "http://o1") (URI. "http://missing-graph1"))
                   (->Quad (URI. "http://s2") (URI. "http://p2") (URI. "http://o2") (URI. "http://missing-graph2"))]
        draftset-info (delete-quads-through-api test-editor draftset-location to-delete)]
    (is (empty? (keys (:changes draftset-info))))))

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
          actual-quads (get-draftset-quads-through-api draftset-location test-editor "false")]
      (is (= (set expected-quads) (set actual-quads))))))

(deftest delete-all-quads-from-draftset-graph
  (let [draftset-location (create-draftset-through-api test-editor)
        initial-statements (statements "test/resources/test-draftset.trig")
        grouped-statements (group-by context initial-statements)
        [graph graph-statements] (first grouped-statements)]
    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")

    (let [draftset-info (delete-quads-through-api test-editor draftset-location graph-statements)
          expected-graphs (set (map :c initial-statements))
          draftset-graphs (tc/key-set (:changes draftset-info))]
      ;;graph should still be in draftset even if it is empty since it should be deleted on publish
      (is (= expected-graphs draftset-graphs)))))

(deftest delete-quads-with-malformed-body
  (let [draftset-location (create-draftset-through-api test-editor)
        body (tc/string->input-stream "NOT NQUADS")
        delete-request (create-delete-quads-request test-editor draftset-location body (.getDefaultMIMEType (formats/->rdf-format :nq)))
        delete-response (route delete-request)
        job-result (tc/await-completion finished-jobs (get-in delete-response [:body :finished-job]))]
    (is (jobs/failed-job-result? job-result))))

(deftest delete-triples-from-graph-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph graph-quads] (first grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]

    (publish-quads-through-api quads)
    (let [draftset-info (delete-quads-through-api test-editor draftset-location [(first graph-quads)])
          draftset-quads (get-draftset-quads-through-api draftset-location test-editor "false")
          expected-quads (eval-statements (rest graph-quads))]
      (is (= #{live-graph} (tc/key-set (:changes draftset-info))))
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest delete-triples-from-graph-not-in-live
  (let [draftset-location (create-draftset-through-api test-editor)
        to-delete [(->Triple (URI. "http://s1") (URI. "http://p1") (URI. "http://o1"))
                   (->Triple (URI. "http://s2") (URI. "http://p2") (URI. "http://o2"))]
        draftset-info (delete-draftset-triples-through-api test-editor draftset-location to-delete (URI. "http://missing"))
        draftset-quads (get-draftset-quads-through-api draftset-location test-editor "false")]

    ;;graph should not exist in draftset since it was not in live
    (is (empty? (:changes draftset-info)))
    (is (empty? draftset-quads))))

(deftest delete-graph-triples-only-in-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-quads (set (statements "test/resources/test-draftset.trig"))
        [graph graph-quads] (first (group-by context draftset-quads))
        quads-to-delete (take 2 graph-quads)
        triples-to-delete (map map->Triple quads-to-delete)]

    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")

    (let [draftset-info (delete-draftset-triples-through-api test-editor draftset-location triples-to-delete graph)
          quads-after-delete (set (get-draftset-quads-through-api draftset-location test-editor))
          expected-quads (set (eval-statements (set/difference draftset-quads quads-to-delete)))]
      (is (= expected-quads quads-after-delete)))))

(deftest delete-all-triples-from-graph
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [graph graph-quads] (first grouped-quads)
        triples-to-delete (map map->Triple graph-quads)
        draftset-location (create-draftset-through-api test-editor)
        draftset-quads (set (statements "test/resources/test-draftset.trig"))]

    (publish-quads-through-api quads)

    (let [draftset-info (delete-draftset-triples-through-api test-editor draftset-location triples-to-delete graph)
          draftset-quads (get-draftset-quads-through-api draftset-location test-editor "false")
          draftset-graphs (tc/key-set (:changes draftset-info))]

      (is (= #{graph} draftset-graphs))
      (is (empty? draftset-quads)))))

(deftest delete-draftset-triples-request-without-graph-parameter
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-quads (statements "test/resources/test-draftset.trig")]
    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")

    (with-open [input-stream (statements->input-stream (take 2 draftset-quads) :nt)]
      (let [delete-request (create-delete-quads-request test-editor draftset-location input-stream (.getDefaultMIMEType (formats/->rdf-format :nt)))
            delete-response (route delete-request)]
        (tc/assert-is-unprocessable-response delete-response)))))

(deftest delete-triples-with-malformed-body
  (let [draftset-location (create-draftset-through-api test-editor)
        body (tc/string->input-stream "NOT TURTLE")
        delete-request (create-delete-quads-request test-editor draftset-location body (.getDefaultMIMEType (formats/->rdf-format :ttl)))
        delete-request (assoc-in delete-request [:params :graph] "http://test-graph")
        delete-response (route delete-request)
        job-result (tc/await-completion finished-jobs (get-in delete-response [:body :finished-job]))]
    (is (jobs/failed-job-result? job-result))))

(deftest delete-draftset-data-for-non-existent-draftset
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [delete-request (tc/with-identity test-manager {:uri "/v1/draftset/missing/data" :request-method :delete :body fs})
          delete-response (route delete-request)]
      (tc/assert-is-not-found-response delete-response))))

(deftest delete-draftset-data-request-with-unknown-content-type
  (with-open [input-stream (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api test-editor)
          delete-request (create-delete-quads-request test-editor draftset-location input-stream "application/unknown-quads-format")
          delete-response (route delete-request)]
      (tc/assert-is-unsupported-media-type-response delete-response))))

(deftest delete-non-existent-live-graph-in-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        graph-to-delete "http://live-graph"
        delete-request (delete-draftset-graph-request test-editor draftset-location "http://live-graph")]

    (testing "silent"
      (let [delete-request (assoc-in delete-request [:params :silent] "true")
            delete-response (route delete-request)]
        (tc/assert-is-ok-response delete-response)))

    (testing "malformed silent flag"
      (let [delete-request (assoc-in delete-request [:params :silent] "invalid")
            delete-response (route delete-request)]
        (tc/assert-is-unprocessable-response delete-response)))

    (testing "not silent"
      (let [delete-request (delete-draftset-graph-request test-editor draftset-location "http://live-graph")
            delete-response (route delete-request)]
        (tc/assert-is-unprocessable-response delete-response)))))

(deftest delete-live-graph-not-in-draftset
  (let [quads (statements "test/resources/test-draftset.trig")
        graph-quads (group-by context quads)
        live-graphs (keys graph-quads)
        graph-to-delete (first live-graphs)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (let [{draftset-graphs :changes} (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)]
      (is (= #{graph-to-delete} (set (keys draftset-graphs)))))))

(deftest delete-graph-with-changes-in-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        published-quad (first graph-quads)
        added-quads (rest graph-quads)]
    (publish-quads-through-api [published-quad])
    (append-quads-to-draftset-through-api test-editor draftset-location added-quads)

    (let [{draftset-graphs :changes} (delete-draftset-graph-through-api test-editor draftset-location graph)]
      (is (= #{graph} (set (keys draftset-graphs)))))))

(deftest delete-graph-only-in-draftset
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api test-editor)
        draftset-quads (statements rdf-data-file)
        grouped-quads (group-by context draftset-quads)
        [graph _] (first grouped-quads)]
    (append-data-to-draftset-through-api test-editor draftset-location rdf-data-file)

    (let [{:keys [changes]} (delete-draftset-graph-through-api test-editor draftset-location graph)
          draftset-graphs (keys changes)
          remaining-quads (eval-statements (get-draftset-quads-through-api draftset-location test-editor))
          expected-quads (eval-statements (mapcat second (rest grouped-quads)))
          expected-graphs (keys grouped-quads)]
      (is (= (set expected-quads) (set remaining-quads)))
      (is (= (set expected-graphs) (set draftset-graphs))))))

(deftest delete-graph-request-for-non-existent-draftset
  (let [request (tc/with-identity test-manager {:uri "/v1/draftset/missing/graph" :request-method :delete :params {:graph "http://some-graph"}})
        response (route request)]
    (tc/assert-is-not-found-response response)))

(deftest delete-graph-by-non-owner

 (let [draftset-location (create-draftset-through-api test-editor)
        [graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (append-quads-to-draftset-through-api test-editor draftset-location quads)

    (let [delete-request (delete-draftset-graph-request test-publisher draftset-location graph)
          delete-response (route delete-request)]
      (tc/assert-is-forbidden-response delete-response))))

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

    (publish-quads-through-api initial-live-quads)
    (append-quads-to-draftset-through-api test-publisher draftset-location appended-quads)
    (publish-draftset-through-api draftset-location test-publisher)

    (let [after-publish-quads (get-live-quads-through-api)]
      (is (= (set (eval-statements quads)) (set after-publish-quads))))))

(deftest publish-draftset-with-statements-deleted-from-graphs-in-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        draftset-location (create-draftset-through-api test-publisher)
        to-delete (map (comp first second) grouped-quads)]
    (publish-quads-through-api quads)
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
    (publish-quads-through-api quads)
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

    (publish-quads-through-api initial-quads)
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
    (publish-quads-through-api graph-quads)
    (publish-draftset-through-api draftset-location test-publisher)

    ;;graph should still exist in live
    (assert-live-quads graph-quads)))

(deftest publish-non-existent-draftset
  (let [response (route (tc/with-identity test-publisher {:uri "/v1/draftset/missing/publish" :request-method :post}))]
    (tc/assert-is-not-found-response response)))

(deftest publish-by-non-publisher-test
  (let [draftset-location (create-draftset-through-api test-editor)]
    (append-quads-to-draftset-through-api test-editor draftset-location (statements "test/resources/test-draftset.trig"))
    (let [publish-response (route (create-publish-request draftset-location test-editor))]
      (tc/assert-is-forbidden-response publish-response))))

(deftest publish-by-non-owner-test
  (let [draftset-location (create-draftset-through-api test-publisher)
        quads (statements "test/resources/test-draftset.trig")]
    (append-quads-to-draftset-through-api test-publisher draftset-location quads)
    (let [publish-request (create-publish-request draftset-location test-manager)
          publish-response (route publish-request)]
      (tc/assert-is-forbidden-response publish-response))))

(defn- create-delete-draftset-request [draftset-location user]
  (tc/with-identity user
    {:uri draftset-location :request-method :delete}))

(deftest delete-draftset-test
  (let [draftset-location (create-draftset-through-api test-editor)
        delete-response (route (create-delete-draftset-request draftset-location test-editor))]
    (tc/assert-is-accepted-response delete-response)
    (tc/await-success finished-jobs (get-in delete-response [:body :finished-job]))

    (let [get-response (route (tc/with-identity test-editor {:uri draftset-location :request-method :get}))]
      (tc/assert-is-not-found-response get-response))))

(deftest delete-non-existent-draftset-test
  (let [delete-response (route (create-delete-draftset-request "/v1/draftset/missing" test-publisher))]
    (tc/assert-is-not-found-response delete-response)))

(deftest delete-draftset-by-non-owner-test
  (let [draftset-location (create-draftset-through-api test-editor)
        delete-response (route (create-delete-draftset-request draftset-location test-manager))]
    (tc/assert-is-forbidden-response delete-response)))

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
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-data-file "test/resources/test-draftset.trig"
        append-response (make-append-data-to-draftset-request test-editor draftset-location draftset-data-file)]
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
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api live-quads)
    (append-quads-to-draftset-through-api test-editor draftset-location ds-quads)
    (let [q "SELECT * WHERE { GRAPH ?c { ?s ?p ?o } }"
          results (select-query-draftset-through-api test-editor draftset-location q :union-with-live? "false")
          expected-quads (eval-statements ds-quads)]
      (is (= (set expected-quads) (set results))))))

(deftest query-draftset-with-malformed-union-with-live
  (let [draftset-location (create-draftset-through-api test-editor)
        q "SELECT * WHERE { ?s ?p ?o }"
        request (create-query-request test-editor draftset-location q "application/sparql-results+json" :union-with-live? "notbool")
        response (route request)]
    (tc/assert-is-unprocessable-response response)))

(deftest query-draftset-unioned-with-live
  (let [test-quads (statements "test/resources/test-draftset.trig")
        grouped-test-quads (group-by context test-quads)
        [live-graph live-quads] (first grouped-test-quads)
        [ds-live-graph draftset-quads] (second grouped-test-quads)
        draftset-location (create-draftset-through-api test-editor)]

    (publish-quads-through-api live-quads)
    (append-quads-to-draftset-through-api test-editor draftset-location draftset-quads)

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
  (let [draftset-location (create-draftset-through-api test-editor)
        response (route (tc/with-identity test-editor {:uri (str draftset-location "/query") :request-method :post}))]
    (tc/assert-is-unprocessable-response response)))

(deftest query-draftset-request-with-invalid-http-method
  (let [draftset-location (create-draftset-through-api test-editor)
        query-request (create-query-request test-editor draftset-location "SELECT * WHERE { ?s ?p ?o }" "text/plain")
        query-request (assoc query-request :request-method :put)
        response (route query-request)]
    (tc/assert-is-method-not-allowed-response response)))

(deftest query-draftset-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        query-request (create-query-request test-publisher draftset-location "SELECT * WHERE { ?s ?p ?o }" "application/sparql-results+json")
        query-response (route query-request)]
    (tc/assert-is-forbidden-response query-response)))

(deftest get-draftset-graph-triples-data
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-data-file "test/resources/test-draftset.trig"
        input-quads (statements draftset-data-file)]
    (append-quads-to-draftset-through-api test-editor draftset-location input-quads)

    (doseq [[graph quads] (group-by context input-quads)]
      (let [graph-triples (set (eval-statements (map map->Triple quads)))
            response-triples (set (get-draftset-graph-triples-through-api draftset-location test-editor graph "false"))]
        (is (= graph-triples response-triples))))))

(deftest get-draftset-quads-data
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-data-file "test/resources/test-draftset.trig"]
    (append-data-to-draftset-through-api test-editor draftset-location draftset-data-file)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location test-editor))
          input-quads (set (eval-statements (statements draftset-data-file)))]
      (is (= input-quads response-quads)))))

(deftest get-draftset-quads-data-with-invalid-accept
  (let [draftset-location (create-draftset-through-api test-editor)]
    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")
    (let [data-request (get-draftset-quads-accept-request draftset-location test-editor "text/invalidrdfformat" "false")
          data-response (route data-request)]
      (tc/assert-is-not-acceptable-response data-response))))

(deftest get-draftset-quads-data-with-multiple-accepted
  (let [draftset-location (create-draftset-through-api test-editor)]
    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")
    (let [accepted "application/n-quads,application/trig,apllication/trix,application/n-triples,application/rdf+xml,text/turtle"
          data-request (get-draftset-quads-accept-request draftset-location test-editor accepted "false")
          data-response (route data-request)]
      (tc/assert-is-ok-response data-response))))

(deftest get-draftset-quads-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (first (keys grouped-quads))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location test-editor "true"))
          expected-quads (set (eval-statements (mapcat second (rest grouped-quads))))]
      (is (= expected-quads response-quads)))))

(deftest get-added-draftset-quads-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        [live-graph live-quads] (first grouped-quads)
        [draftset-graph draftset-quads] (second grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api live-quads)
    (append-quads-to-draftset-through-api test-editor draftset-location draftset-quads)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location test-editor "true"))
          expected-quads (set (eval-statements (concat live-quads draftset-quads)))]
      (is (= expected-quads response-quads)))))

(deftest get-draftset-triples-for-deleted-graph-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        grouped-quads (group-by context quads)
        graph-to-delete (ffirst grouped-quads)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)

    (let [draftset-triples (get-draftset-graph-triples-through-api draftset-location test-editor graph-to-delete "true")]
      (is (empty? draftset-triples)))))

(deftest get-draftset-triples-for-published-graph-not-in-draftset-unioned-with-live
  (let [quads (statements "test/resources/test-draftset.trig")
        [graph graph-quads] (first (group-by context quads))
        draftset-location (create-draftset-through-api)]
    (publish-quads-through-api graph-quads)

    (let [draftset-graph-triples (get-draftset-graph-triples-through-api draftset-location test-editor graph "true")
          expected-triples (eval-statements (map map->Triple graph-quads))]
      (is (= (set expected-triples) (set draftset-graph-triples))))))

(deftest get-draftset-graph-triples-request-without-graph
  (let [draftset-location (create-draftset-through-api test-editor)]
    (append-quads-to-draftset-through-api test-editor draftset-location (statements "test/resources/test-draftset.trig"))
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
  (let [draftset-location (create-draftset-through-api test-editor)
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
  (let [meta-request (create-update-draftset-metadata-request test-manager "/v1/draftset/missing" "Title!" "Description")
        meta-response (route meta-request)]
    (tc/assert-is-not-found-response meta-response)))

(deftest set-metadata-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor "Test draftset" "Test description")
        update-request (create-update-draftset-metadata-request test-publisher draftset-location "New title" "New description")
        update-response (route update-request)]
    (tc/assert-is-forbidden-response update-response)))

(deftest submit-draftset-to-role
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-request (create-submit-to-role-request test-editor draftset-location :publisher)
        {ds-info :body :as submit-response} (route submit-request)]
    (tc/assert-is-ok-response submit-response)
    (tc/assert-schema Draftset ds-info)

    (is (= false (contains? ds-info :current-owner))))
  )

(deftest submit-non-existent-draftset-to-role
  (let [submit-response (route (create-submit-to-role-request test-editor "/v1/draftset/missing" :publisher))]
    (tc/assert-is-not-found-response submit-response)))

(deftest submit-draftset-to-role-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-response (route (create-submit-to-role-request test-publisher draftset-location :manager))]
    (tc/assert-is-forbidden-response submit-response)))

(deftest submit-draftset-to-invalid-role
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-response (route (create-submit-to-role-request test-editor draftset-location :invalid))]
    (tc/assert-is-unprocessable-response submit-response)))

(deftest submit-draftset-to-user
  (let [draftset-location (create-draftset-through-api test-editor)
        {:keys [body] :as submit-response} (route (submit-draftset-to-user-request draftset-location test-publisher test-editor))]
    (tc/assert-is-ok-response submit-response)
    (tc/assert-schema Draftset body)

    (let [{:keys [current-owner claim-user] :as ds-info} body]
      (is (nil? current-owner))
      (is (= (user/username test-publisher) claim-user)))))

(deftest submit-draftset-to-user-as-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-response (route (submit-draftset-to-user-request draftset-location test-manager test-publisher))]
    (tc/assert-is-forbidden-response submit-response)))

(deftest submit-non-existent-draftset-to-user
  (let [submit-response (route (submit-draftset-to-user-request "/v1/draftset/missing" test-publisher test-editor))]
    (tc/assert-is-not-found-response submit-response)))

(deftest submit-draftset-to-non-existent-user
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-response (route (submit-draftset-to-username-request draftset-location "invalid-user@example.com" test-editor))]
    (tc/assert-is-unprocessable-response submit-response)))

(deftest submit-draftset-without-user-param
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-request (submit-draftset-to-user-request draftset-location test-publisher test-editor)
        submit-request (update-in submit-request [:params] dissoc :user)
        response (route submit-request)]
    (tc/assert-is-unprocessable-response response)))

(deftest submit-to-with-both-user-and-role-params
  (let [draftset-location (create-draftset-through-api test-editor)
        request (submit-draftset-to-user-request draftset-location test-publisher test-editor)
        request (assoc-in request [:params :role] "editor")
        response (route request)]
    (tc/assert-is-unprocessable-response response)))

(deftest get-options-test
  (let [draftset-location (create-draftset-through-api test-editor)
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
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location live-graph)

    (let [{:keys [changes]} (get-draftset-info-through-api draftset-location test-editor)]
      (is (= #{live-graph} (tc/key-set changes))))

    (let [{:keys [changes] :as ds-info} (revert-draftset-graph-changes-through-api draftset-location test-editor live-graph)]
      (is (= #{} (tc/key-set changes))))

    (let [ds-quads (get-draftset-quads-through-api draftset-location test-editor "true")]
      (is (= (set (eval-statements quads)) (set ds-quads))))))

(deftest revert-graph-change-in-unowned-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location live-graph)

    (let [revert-request (revert-draftset-graph-changes-request draftset-location test-publisher live-graph)
          response (route revert-request)]
      (tc/assert-is-forbidden-response response))))

(deftest revert-graph-change-in-draftset-unauthorised
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location live-graph)

    (let [revert-request {:uri (str draftset-location "/changes") :request-method :delete :params {:graph live-graph}}
          response (route revert-request)]
      (tc/assert-is-unauthorised-response response))))

(deftest revert-non-existent-graph-change-in-draftest
  (let [draftset-location (create-draftset-through-api test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "http://missing")
        response (route revert-request)]
    (tc/assert-is-not-found-response response)))

(deftest revert-change-in-non-existent-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (publish-quads-through-api quads)
    (let [revert-request (revert-draftset-graph-changes-request "/v1/draftset/missing" test-manager live-graph)
          response (route revert-request)]
      (tc/assert-is-not-found-response response))))

(deftest revert-graph-change-request-without-graph-parameter
  (let [draftset-location (create-draftset-through-api test-editor)
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
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (copy-live-graph-into-draftset draftset-location test-editor live-graph)

    (let [ds-quads (get-draftset-quads-through-api draftset-location test-editor "false")
          expected-quads (eval-statements quads)]
      (is (= (set expected-quads) (set ds-quads))))))

(deftest copy-live-graph-with-existing-draft-into-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        [published private] (split-at 2 quads)
        draftset-location (create-draftset-through-api test-editor)]

    ;;publish some graph quads to live and some others into the draftset
    (publish-quads-through-api published)
    (append-quads-to-draftset-through-api test-editor draftset-location private)

    ;;copy live graph into draftset
    (copy-live-graph-into-draftset draftset-location test-editor live-graph)

    ;;draftset graph should contain only the publish quads
    (let [graph-quads (get-draftset-quads-through-api draftset-location test-editor "false")]
      (is (= (set (eval-statements published)) (set graph-quads))))))

(deftest copy-live-graph-into-unowned-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (let [copy-request (copy-live-graph-into-draftset-request draftset-location test-publisher live-graph)
          copy-response (route copy-request)]
      (tc/assert-is-forbidden-response copy-response))))

(deftest copy-non-existent-live-graph
  (let [draftset-location (create-draftset-through-api test-editor)
        copy-request (copy-live-graph-into-draftset-request draftset-location test-editor "http://missing")
        copy-response (route copy-request)]
    (tc/assert-is-unprocessable-response copy-response)))

(deftest copy-live-graph-into-non-existent-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (publish-quads-through-api quads)
    (let [copy-request (copy-live-graph-into-draftset-request "/v1/draftset/missing" test-publisher live-graph)
          copy-response (route copy-request)]
      (tc/assert-is-not-found-response copy-response))))

(deftest draftset-graphs-state-test
  (testing "Graph created"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (create-draftset-through-api test-editor)]
      (append-quads-to-draftset-through-api test-editor draftset-location quads)
      (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api draftset-location test-editor)]
        (is (= :created (get-in changes [live-graph :status]))))))

  (testing "Quads deleted from live graph"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (create-draftset-through-api test-editor)]
      (publish-quads-through-api quads)
      (delete-quads-through-api test-editor draftset-location (take 1 quads))

      (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api draftset-location test-editor)]
        (is (= :updated (get-in changes [live-graph :status]))))))

  (testing "Quads added to live graph"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          [published to-add] (split-at 1 quads)
          draftset-location (create-draftset-through-api test-editor)]
      (publish-quads-through-api published)
      (append-quads-to-draftset-through-api test-editor draftset-location to-add)

      (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api draftset-location test-editor)]
        (is (= :updated (get-in changes [live-graph :status]))))))

  (testing "Graph deleted"
    (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
          draftset-location (create-draftset-through-api test-editor)]
      (publish-quads-through-api quads)
      (delete-draftset-graph-through-api test-editor draftset-location live-graph)

      (let [{:keys [changes] :as ds-info} (get-draftset-info-through-api draftset-location test-editor)]
        (is (= :deleted (get-in changes [live-graph :status])))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Handler tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ok-response->typed-body [schema {:keys [body] :as response}]
  (tc/assert-is-ok-response response)
  (tc/assert-schema schema body)
  body)
