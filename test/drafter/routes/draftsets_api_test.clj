(ns drafter.routes.draftsets-api-test
  (:require [drafter.test-common :refer :all]
            [clojure.test :refer :all]
            [clojure.set :as set]
            [drafter.routes.draftsets-api :refer :all]
            [drafter.rdf.drafter-ontology :refer [drafter:DraftGraph drafter:modifiedAt]]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-publisher test-manager test-password]]
            [drafter.user.repository :as user-repo]
            [drafter.user.memory-repository :as memrepo]
            [grafter.rdf :refer [statements context add]]
            [grafter.rdf.io :refer [rdf-serializer]]
            [grafter.rdf.formats :as formats]
            [grafter.rdf.protocols :refer [->Triple map->Triple ->Quad map->Quad]]
            [grafter.rdf.repository :as repo]
            [drafter.rdf.draft-management.jobs :as jobs]
            [drafter.util :as util]
            [clojure.java.io :as io]
            [schema.core :as s]
            [swirrl-server.async.jobs :refer [finished-jobs]])
  (:import [java.util Date]
           [java.io ByteArrayOutputStream ByteArrayInputStream BufferedReader]
           [org.openrdf.query QueryResultHandler]
           [org.openrdf.query.resultio.sparqljson SPARQLResultsJSONParser]
           [org.openrdf.query.resultio.text.csv SPARQLResultsCSVParser]))

(def ^:private ^:dynamic *route*)
(def ^:private ^:dynamic *user-repo*)

(defn is-client-error-response?
  "Whether the given ring response map represents a client error."
  [{:keys [status] :as response}]
  (and (>= status 400)
       (< status 500)))

(defn- route [request]
  (*route* request))

(defn- statements->input-stream [statements format]
  (let [bos (ByteArrayOutputStream.)
        serialiser (rdf-serializer bos :format format)]
    (add serialiser statements)
    (ByteArrayInputStream. (.toByteArray bos))))

(defn- with-identity [user request]
  (let [unencoded-auth (str (user/username user) ":" test-password)
        encoded-auth (buddy.core.codecs/str->base64 unencoded-auth)]
    (-> request
        (assoc :identity user)
        (assoc-in [:headers "Authorization"] (str "Basic " encoded-auth)))))

(defn- append-to-draftset-request [user draftset-location data-stream content-type]
  (with-identity user
    {:uri (str draftset-location "/data")
     :request-method :put
     :body data-stream
     :headers {"content-type" content-type}}))

(defn- create-draftset-request
  ([] (create-draftset-request test-editor))
  ([user] (create-draftset-request user nil))
  ([user display-name] (create-draftset-request user display-name nil))
  ([user display-name description]
   (with-identity user {:uri "/v1/draftsets" :request-method :post :params {:display-name display-name :description description}})))

(defn- make-append-data-to-draftset-request [user draftset-location data-file-path]
  (with-open [fs (io/input-stream data-file-path)]
    (let [request (append-to-draftset-request user draftset-location fs "application/x-trig")]
      (route request))))

(defn- append-data-to-draftset-through-api [user draftset-location draftset-data-file]
  (let [append-response (make-append-data-to-draftset-request user draftset-location draftset-data-file)]
    (await-success finished-jobs (:finished-job (:body append-response)))))

(defn- statements->append-request [user draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)]
    (append-to-draftset-request user draftset-location input-stream (.getDefaultMIMEType format))))

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

(def see-other-response-schema
  (merge ring-response-schema
         {:status (s/eq 303)
          :headers {(s/required-key "Location") s/Str}}))

(defn assert-is-see-other-response [response]
  (assert-schema see-other-response-schema response))

(def ^:private DraftsetWithoutTitleOrDescription
  {:id s/Str
   :changes {s/Str {s/Any s/Any}}
   :created-at Date
   :created-by s/Str
   (s/optional-key :current-owner) s/Str
   (s/optional-key :claim-role) s/Keyword
   (s/optional-key :claim-user) s/Str
   (s/optional-key :submitted-by) s/Str})

(def ^:private DraftsetWithoutDescription
  (assoc DraftsetWithoutTitleOrDescription :display-name s/Str))

(def ^:private draftset-with-description-info-schema
  (assoc DraftsetWithoutDescription :description s/Str))

(def ^:private Draftset
  (merge DraftsetWithoutTitleOrDescription
         {(s/optional-key :description) s/Str
          (s/optional-key :display-name) s/Str}))

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

(defn- get-draftset-quads-request [draftset-location user format union-with-live?-str]
  (with-identity user
    {:uri (str draftset-location "/data")
     :request-method :get
     :headers {"accept" (.getDefaultMIMEType format)}
     :params {:union-with-live union-with-live?-str}}))

(defn- get-draftset-quads-through-api
  ([draftset-location user]
   (get-draftset-quads-through-api draftset-location user "false"))
  ([draftset-location user union-with-live?]
   (let [data-request (get-draftset-quads-request draftset-location user formats/rdf-nquads union-with-live?)
         data-response (route data-request)]
     (assert-is-ok-response data-response)
     (concrete-statements (:body data-response) formats/rdf-nquads))))

(defn- get-draftset-graph-triples-through-api [draftset-location user graph union-with-live?-str]
  (let [data-request {:uri (str draftset-location "/data")
                      :request-method :get
                      :headers {"accept" "application/n-triples"}
                      :params {:union-with-live union-with-live?-str :graph graph}}
        data-request (with-identity user data-request)
        {:keys [body] :as data-response} (route data-request)]
    (assert-is-ok-response data-response)
    (concrete-statements body formats/rdf-ntriples)))

(defn- create-publish-request [draftset-location user]
  (with-identity user {:uri (str draftset-location "/publish") :request-method :post}))

(defn- publish-draftset-through-api [draftset-location user]
  (let [publish-request (create-publish-request draftset-location user)
        publish-response (route publish-request)]
    (await-success finished-jobs (:finished-job (:body publish-response)))))

(defn- publish-quads-through-api [quads]
  (let [draftset-location (create-draftset-through-api test-publisher)]
    (append-quads-to-draftset-through-api test-publisher draftset-location quads)
    (publish-draftset-through-api draftset-location test-publisher)))

(defn- create-delete-quads-request [user draftset-location input-stream format]
  (with-identity user {:uri (str draftset-location "/data")
                       :request-method :delete
                       :body input-stream
                       :headers {"content-type" format}}))

(defn- get-draftset-info-request [draftset-location user]
  (with-identity user {:uri draftset-location :request-method :get}))

(defn- get-draftset-info-through-api [draftset-location user]
  (let [{:keys [body] :as response} (route (get-draftset-info-request draftset-location user))]
    (assert-is-ok-response response)
    (assert-schema Draftset body)
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
  (let [input-stream (statements->input-stream statements format)]
    (create-delete-quads-request user draftset-location input-stream (.getDefaultMIMEType format))))

(defn- create-delete-triples-request [user draftset-location statements graph]
  (assoc-in (create-delete-statements-request user draftset-location statements formats/rdf-ntriples)
            [:params :graph] graph))

(defn- delete-triples-through-api [user draftset-location triples graph]
  (-> (create-delete-triples-request user draftset-location triples graph)
      route
      await-delete-statements-response))

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
  (let [tmp-ds (create-draftset-through-api test-editor)]
    (get-draftset-quads-through-api tmp-ds test-editor "true")))

(defn- assert-live-quads [expected-quads]
  (let [live-quads (get-live-quads-through-api)]
    (is (= (set (eval-statements expected-quads)) (set live-quads)))))

(defn- create-submit-request [user draftset-location role]
  (with-identity user {:uri (str draftset-location "/submit-to") :request-method :post :params {:role (name role)}}))

(defn- submit-draftset-through-api [user draftset-location role]
  (let [response (route (create-submit-request user draftset-location role))]
    (assert-is-ok-response response)))

(deftest create-draftset-without-title-or-description
  (let [response (route (with-identity test-editor {:uri "/v1/draftsets" :request-method :post}))]
    (assert-is-see-other-response response)))

(deftest create-draftset-with-title-and-without-description
  (let [response (route (create-draftset-request test-editor "Test Title!"))]
    (assert-is-see-other-response response)))

(deftest create-draftset-with-title-and-description
  (let [response (route (create-draftset-request test-editor "Test title" "Test description"))]
    (assert-is-see-other-response response)))

(defn- get-all-draftsets-through-api [user]
  (let [request (with-identity user {:uri "/v1/draftsets" :request-method :get})
        {:keys [body] :as response} (route request)]
    (assert-is-ok-response response)
    body))

(defn- submit-to-username-request [draftset-location target-username user]
  (with-identity user
    {:uri (str draftset-location "/submit-to") :request-method :post :params {:user target-username}}))

(defn- submit-to-user-request [draftset-location target-user user]
  (submit-to-username-request draftset-location (user/username target-user) user))

(defn- submit-to-user-through-api [draftset-location target-user user]
  (let [request (submit-to-user-request draftset-location target-user user)
        response (route request)]
    (assert-is-ok-response response)))

(defn- create-claim-request [draftset-location user]
  (with-identity user {:uri (str draftset-location "/claim") :request-method :put}))

(defn- claim-draftset-through-api [draftset-location user]
  (let [claim-request (create-claim-request draftset-location user)
        {:keys [body] :as claim-response} (route claim-request)]
      (assert-is-ok-response claim-response)
      (assert-schema Draftset body)
      body))

(deftest get-all-draftsets-test
  (let [owned-ds (create-draftset-through-api test-publisher "owned")
        editing-ds (create-draftset-through-api test-editor "editing")
        claimable-publisher-ds (create-draftset-through-api test-editor "publishing")
        claimable-manager-ds (create-draftset-through-api test-editor "admining")]

    ;;submit two draftsets, one to publishers, the other to managers
    (submit-draftset-through-api test-editor claimable-publisher-ds :publisher)
    (submit-draftset-through-api test-editor claimable-manager-ds :manager)

    (let [ds-infos (get-all-draftsets-through-api test-publisher)
          available-names (set (map :display-name ds-infos))]
      (assert-schema [Draftset] ds-infos)
      (is (= #{"owned" "publishing"} available-names)))))

(deftest get-empty-draftset-without-title-or-description
  (let [draftset-location (create-draftset-through-api test-editor)
        ds-info (get-draftset-info-through-api draftset-location test-editor)]
    (assert-schema DraftsetWithoutTitleOrDescription ds-info)))

(deftest get-empty-draftset-without-description
  (let [display-name "Test title!"
        draftset-location (create-draftset-through-api test-editor display-name)
        ds-info (get-draftset-info-through-api draftset-location test-editor)]
    (assert-schema DraftsetWithoutDescription ds-info)
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
      (assert-schema DraftsetWithoutDescription ds-info)

      (is (= display-name (:display-name ds-info)))
      (is (= live-graphs (key-set (:changes ds-info)))))))

(deftest get-draftset-request-for-non-existent-draftset
  (let [response (route (get-draftset-info-request "/v1/draftset/missing" test-publisher))]
    (assert-is-not-found-response response)))

(deftest get-draftset-available-for-claim
  (let [draftset-location (create-draftset-through-api test-editor)]
    (submit-draftset-through-api test-editor draftset-location :publisher)
    (let [ds-info (get-draftset-info-through-api draftset-location test-publisher)])))

(deftest get-draftset-for-other-user-test
  (let [draftset-location (create-draftset-through-api test-editor)
        get-request (get-draftset-info-request draftset-location test-publisher)
        get-response (route get-request)]
    (assert-is-forbidden-response get-response)))

(defn- get-claimable-draftsets-through-api [user]
  (let [request (with-identity user {:uri "/v1/draftsets/claimable" :request-method :get})
        {:keys [body] :as response} (route request)]
    (assert-is-ok-response response)
    (assert-schema [Draftset] body)
    body))

(deftest get-claimable-draftsets-test
  (let [ds-names (map #(str "Draftset " %) (range 1 6))
        [ds1 ds2 ds3 ds4 ds5] (doall (map #(create-draftset-through-api test-editor %) ds-names))]
    (submit-draftset-through-api test-editor ds1 :editor)
    (submit-draftset-through-api test-editor ds2 :publisher)
    (submit-draftset-through-api test-editor ds3 :manager)
    (submit-to-user-through-api ds5 test-publisher test-editor)

    ;;editor should be able to claim all draftsets just submitted as they have not been claimed
    (let [editor-claimable (get-claimable-draftsets-through-api test-editor)]
      (let [expected-claimable-names (map #(nth ds-names %) [0 1 2 4])
            claimable-names (map :display-name editor-claimable)]
        (is (= (set expected-claimable-names) (set claimable-names)))))

    (let [publisher-claimable (get-claimable-draftsets-through-api test-publisher)]
      ;;Draftsets 1, 2 and 5 should be on submit to publisher
      ;;Draftset 3 is in too high a role
      ;;Draftset 4 is not available
      (let [claimable-names (map :display-name publisher-claimable)
            expected-claimable-names (map #(nth ds-names %) [0 1 4])]
        (is (= (set expected-claimable-names) (set claimable-names)))))

    (doseq [ds [ds1 ds3]]
      (claim-draftset-through-api ds test-manager))
    
    (claim-draftset-through-api ds5 test-publisher)

    ;;editor should not be able to see ds1, ds3 or ds5 after they have been claimed
    (let [editor-claimable (get-claimable-draftsets-through-api test-editor)]
      (is (= 1 (count editor-claimable)))
      (is (= (:display-name (first editor-claimable)) (nth ds-names 1))))))

(deftest append-quad-data-with-valid-content-type-to-draftset
  (let [data-file-path "test/resources/test-draftset.trig"
        quads (statements data-file-path)
        draftset-location (create-draftset-through-api test-editor)]
    (append-quads-to-draftset-through-api test-editor draftset-location quads)
    (let [draftset-graphs (key-set (:changes (get-draftset-info-through-api draftset-location test-editor)))
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
                                      (-> *test-backend*
                                          (repo/query
                                           (str "SELECT ?modified {"
                                                "   ?draftgraph a <" drafter:DraftGraph "> ;"
                                                "                 <" drafter:modifiedAt ">   ?modified ."
                                                "}"))
                                          first
                                          (get "modified")
                                          .calendarValue
                                          .toGregorianCalendar
                                          .getTime))]

    (testing "Publishing some triples sets the modified time"
      (append-triples-to-draftset-through-api test-editor draftset-location quads "http://foo/")

      (let [first-timestamp (get-draft-graph-modified-at)]
        (is (instance? Date first-timestamp))

        (testing "Publishing more triples afterwards updates the modified time"
          (Thread/sleep 500)

          (append-triples-to-draftset-through-api test-editor draftset-location quads "http://foo/")

          (let [second-timestamp (get-draft-graph-modified-at)]
            (is (instance? Date second-timestamp))

            (is (< (.getTime first-timestamp)
                   (.getTime second-timestamp))
                "Modified time is updated")


            (Thread/sleep 500)

            (delete-triples-through-api test-editor draftset-location quads "http://foo/")
            (let [third-timestamp (get-draft-graph-modified-at)]

              (is (< (.getTime second-timestamp)
                     (.getTime third-timestamp))
                  "Modified time is updated"))))))))

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
        to-delete [(->Quad "http://s1" "http://p1" "http://o1" "http://missing-graph1")
                   (->Quad "http://s2" "http://p2" "http://o2" "http://missing-graph2")]
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
          draftset-graphs (key-set (:changes draftset-info))]
      ;;graph should still be in draftset even if it is empty since it should be deleted on publish
      (is (= expected-graphs draftset-graphs)))))

(deftest delete-quads-with-malformed-body
  (let [draftset-location (create-draftset-through-api test-editor)
        body (string->input-stream "NOT NQUADS")
        delete-request (create-delete-quads-request test-editor draftset-location body (.getDefaultMIMEType formats/rdf-nquads))
        delete-response (route delete-request)
        job-result (await-completion finished-jobs (get-in delete-response [:body :finished-job]))]
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
      (is (= #{live-graph} (key-set (:changes draftset-info))))
      (is (= (set expected-quads) (set draftset-quads))))))

(deftest delete-triples-from-graph-not-in-live
  (let [draftset-location (create-draftset-through-api test-editor)
        to-delete [(->Triple "http://s1" "http://p1" "http://o1")
                   (->Triple "http://s2" "http://p2" "http://o2")]
        draftset-info (delete-draftset-triples-through-api test-editor draftset-location to-delete "http://missing")
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
          draftset-graphs (key-set (:changes draftset-info))]

      (is (= #{graph} draftset-graphs))
      (is (empty? draftset-quads)))))

(deftest delete-draftset-triples-request-without-graph-parameter
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-quads (statements "test/resources/test-draftset.trig")]
    (append-data-to-draftset-through-api test-editor draftset-location "test/resources/test-draftset.trig")

    (with-open [input-stream (statements->input-stream (take 2 draftset-quads) formats/rdf-ntriples)]
      (let [delete-request (create-delete-quads-request test-editor draftset-location input-stream (.getDefaultMIMEType formats/rdf-ntriples))
            delete-response (route delete-request)]
        (assert-is-unprocessable-response delete-response)))))

(deftest delete-triples-with-malformed-body
  (let [draftset-location (create-draftset-through-api test-editor)
        body (string->input-stream "NOT TURTLE")
        delete-request (create-delete-quads-request test-editor draftset-location body (.getDefaultMIMEType formats/rdf-turtle))
        delete-request (assoc-in delete-request [:params :graph] "http://test-graph")
        delete-response (route delete-request)
        job-result (await-completion finished-jobs (get-in delete-response [:body :finished-job]))]
    (is (jobs/failed-job-result? job-result))))

(deftest delete-draftset-data-for-non-existent-draftset
  (with-open [fs (io/input-stream "test/resources/test-draftset.trig")]
    (let [delete-request (with-identity test-manager {:uri "/v1/draftset/missing/data" :request-method :delete :body fs})
          delete-response (route delete-request)]
      (assert-is-not-found-response delete-response))))

(deftest delete-draftset-data-request-with-unknown-content-type
  (with-open [input-stream (io/input-stream "test/resources/test-draftset.trig")]
    (let [draftset-location (create-draftset-through-api test-editor)
          delete-request (create-delete-quads-request test-editor draftset-location input-stream "application/unknown-quads-format")
          delete-response (route delete-request)]
      (assert-is-unsupported-media-type-response delete-response))))

(deftest delete-non-existent-live-graph-in-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        graph-to-delete "http://live-graph"
        delete-request (delete-draftset-graph-request test-editor draftset-location "http://live-graph")
        delete-response (route delete-request)]

    (assert-is-unprocessable-response delete-response)))

(deftest delete-live-graph-not-in-draftset
  (let [quads (statements "test/resources/test-draftset.trig")
        graph-quads (group-by context quads)
        live-graphs (keys graph-quads)
        graph-to-delete (first live-graphs)
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location graph-to-delete)

    (let [{draftset-graphs :changes} (get-draftset-info-through-api draftset-location test-editor)]
      (is (= #{graph-to-delete} (set (keys draftset-graphs)))))))

(deftest delete-graph-with-changes-in-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        [graph graph-quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        published-quad (first graph-quads)
        added-quads (rest graph-quads)]
    (publish-quads-through-api [published-quad])
    (append-quads-to-draftset-through-api test-editor draftset-location added-quads)
    (delete-draftset-graph-through-api test-editor draftset-location graph)

    (let [{draftset-graphs :changes} (get-draftset-info-through-api draftset-location test-editor)]
      (is (= #{graph} (set (keys draftset-graphs)))))))

(deftest delete-graph-only-in-draftset
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api test-editor)
        draftset-quads (statements rdf-data-file)
        grouped-quads (group-by context draftset-quads)
        [graph _] (first grouped-quads)]
    (append-data-to-draftset-through-api test-editor draftset-location rdf-data-file)

    (delete-draftset-graph-through-api test-editor draftset-location graph)

    (let [remaining-quads (eval-statements (get-draftset-quads-through-api draftset-location test-editor))
          expected-quads (eval-statements (mapcat second (rest grouped-quads)))]
      (is (= (set expected-quads) (set remaining-quads))))

    (let [draftset-info (get-draftset-info-through-api draftset-location test-editor)
          draftset-graphs (keys (:changes draftset-info))
          expected-graphs (keys grouped-quads)]
      (is (= (set expected-graphs) (set draftset-graphs))))))

(deftest delete-graph-request-for-non-existent-draftset
  (let [request (with-identity test-manager {:uri "/v1/draftset/missing/graph" :request-method :delete :params {:graph "http://some-graph"}})
        response (route request)]
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
  (let [response (route (with-identity test-publisher {:uri "/v1/draftset/missing/publish" :request-method :post}))]
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
  (with-identity user
    {:uri draftset-location :request-method :delete}))

(deftest delete-draftset-test
  (let [rdf-data-file "test/resources/test-draftset.trig"
        draftset-location (create-draftset-through-api test-editor)
        delete-response (route (create-delete-draftset-request draftset-location test-editor))]
    (assert-is-ok-response delete-response)

    (let [get-response (route (with-identity test-editor {:uri draftset-location :request-method :get}))]
      (assert-is-not-found-response get-response))))

(deftest delete-non-existent-draftset-test
  (let [delete-response (route (create-delete-draftset-request "/v1/draftset/missing" test-publisher))]
    (assert-is-not-found-response delete-response)))

(deftest delete-draftset-by-non-owner-test
  (let [draftset-location (create-draftset-through-api test-editor)
        delete-response (route (create-delete-draftset-request draftset-location test-manager))]
    (assert-is-forbidden-response delete-response)))

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
  (with-identity user
    {:uri (str draftset-location "/query")
     :headers {"accept" accept-content-type}
     :request-method :post
     :params {:query query :union-with-live union-with-live?}}))

(defn- select-query-draftset-through-api [user draftset-location select-query & {:keys [union-with-live?]}]
  (let [request (create-query-request user draftset-location select-query "application/sparql-results+json" :union-with-live? union-with-live?)
        {:keys [body] :as query-response} (route request)]
    (assert-is-ok-response query-response)
    (let [result-state (atom #{})
          result-handler (result-set-handler result-state)
          parser (doto (SPARQLResultsJSONParser.) (.setQueryResultHandler result-handler))]
      (.parse parser body)
      @result-state)))

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
    (assert-is-unprocessable-response response)))

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
    (assert-is-not-found-response response)))

(deftest query-draftset-request-with-missing-query-parameter
  (let [draftset-location (create-draftset-through-api test-editor)
        response (route (with-identity test-editor {:uri (str draftset-location "/query") :request-method :post}))]
    (assert-is-unprocessable-response response)))

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
            response-triples (set (get-draftset-graph-triples-through-api draftset-location test-editor graph "false"))]
        (is (= graph-triples response-triples))))))

(deftest get-draftset-quads-data
  (let [draftset-location (create-draftset-through-api test-editor)
        draftset-data-file "test/resources/test-draftset.trig"]
    (append-data-to-draftset-through-api test-editor draftset-location draftset-data-file)

    (let [response-quads (set (get-draftset-quads-through-api draftset-location test-editor))
          input-quads (set (eval-statements (statements draftset-data-file)))]
      (is (= input-quads response-quads)))))

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
          data-request (with-identity test-editor data-request)
          data-response (route data-request)]
      (assert-is-unprocessable-response data-response))))

(deftest get-draftset-data-for-missing-draftset
  (let [response (route (with-identity test-manager {:uri "/v1/draftset/missing/data" :request-method :get :headers {"accept" "application/n-quads"}}))]
    (assert-is-not-found-response response)))

(deftest get-draftset-data-for-unowned-draftset
  (let [draftset-location (create-draftset-through-api test-editor)
        get-data-request (get-draftset-quads-request draftset-location test-publisher formats/rdf-nquads "false")
        response (route get-data-request)]
    (assert-is-forbidden-response response)))

(defn- create-update-draftset-metadata-request [user draftset-location title description]
  (with-identity user
    {:uri draftset-location :request-method :put :params {:display-name title :description description}}))

(defn- update-draftset-metadata-through-api [user draftset-location title description]
  (let [request (create-update-draftset-metadata-request user draftset-location title description)
        {:keys [body] :as response} (route request)]
    (assert-is-ok-response response)
    (assert-schema Draftset body)
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
    (assert-is-not-found-response meta-response)))

(deftest set-metadata-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor "Test draftset" "Test description")
        update-request (create-update-draftset-metadata-request test-publisher draftset-location "New title" "New description")
        update-response (route update-request)]
    (assert-is-forbidden-response update-response)))

(deftest submit-draftset-to-role
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-request (create-submit-request test-editor draftset-location :publisher)
        submit-response (route submit-request)]
    (assert-is-ok-response submit-response)

    ;;NOTE: user should still be able to get draftset info as it has not yet been claimed
    (let [ds-info (get-draftset-info-through-api draftset-location test-editor)]
      (is (= false (contains? ds-info :current-owner))))))

(deftest submit-non-existent-draftset-to-role
  (let [submit-response (route (create-submit-request test-editor "/v1/draftset/missing" :publisher))]
    (assert-is-not-found-response submit-response)))

(deftest submit-draftset-to-role-by-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-response (route (create-submit-request test-publisher draftset-location :manager))]
    (assert-is-forbidden-response submit-response)))

(deftest submit-draftset-to-invalid-role
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-response (route (create-submit-request test-editor draftset-location :invalid))]
    (assert-is-unprocessable-response submit-response)))

(deftest submit-draftset-to-user
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-response (route (submit-to-user-request draftset-location test-publisher test-editor))]
    (assert-is-ok-response submit-response)

    (let [{:keys [current-owner claim-user] :as ds-info} (get-draftset-info-through-api draftset-location test-publisher)]
      (is (nil? current-owner))
      (is (= (user/username test-publisher) claim-user)))))

(deftest submit-draftset-to-user-as-non-owner
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-response (route (submit-to-user-request draftset-location test-manager test-publisher))]
    (assert-is-forbidden-response submit-response)))

(deftest submit-non-existent-draftset-to-user
  (let [submit-response (route (submit-to-user-request "/v1/draftset/missing" test-publisher test-editor))]
    (assert-is-not-found-response submit-response)))

(deftest submit-draftset-to-non-existent-user
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-response (route (submit-to-username-request draftset-location "invalid-user@example.com" test-editor))]
    (assert-is-unprocessable-response submit-response)))

(deftest submit-draftset-without-user-param
  (let [draftset-location (create-draftset-through-api test-editor)
        submit-request (submit-to-user-request draftset-location test-publisher test-editor)
        submit-request (update-in submit-request [:params] dissoc :user)
        response (route submit-request)]
    (assert-is-unprocessable-response response)))

(deftest submit-to-with-both-user-and-role-params
  (let [draftset-location (create-draftset-through-api test-editor)
        request (submit-to-user-request draftset-location test-publisher test-editor)
        request (assoc-in request [:params :role] "editor")
        response (route request)]
    (assert-is-unprocessable-response response)))

(deftest claim-draftset-submitted-to-role
  (let [draftset-location (create-draftset-through-api test-editor)]
    (submit-draftset-through-api test-editor draftset-location :publisher)

    (let [{:keys [current-owner] :as ds-info} (claim-draftset-through-api draftset-location test-publisher)]
      (is (= (user/username test-publisher) current-owner)))))

(deftest claim-draftset-submitted-to-user
  (let [draftset-location (create-draftset-through-api test-editor)]
    (submit-to-user-through-api draftset-location test-publisher test-editor)
  
    (let [{:keys [current-owner claim-user] :as ds-info} (claim-draftset-through-api draftset-location test-publisher)]
      (is (= (user/username test-publisher current-owner)))
      (is (nil? claim-user)))))

(deftest claim-draftset-submitted-to-other-user
  (let [draftset-location (create-draftset-through-api test-editor)]
    (submit-to-user-through-api draftset-location test-publisher test-editor)
    (let [claim-request (create-claim-request draftset-location test-manager)
          claim-response (route claim-request)]
      (assert-is-forbidden-response claim-response))))

(deftest claim-draftset-owned-by-self
  (let [draftset-location (create-draftset-through-api test-editor)
        claim-request (create-claim-request draftset-location test-editor)
        {:keys [body] :as claim-response} (route claim-request)]
    (assert-is-ok-response claim-response)
    (is (= (user/username test-editor (:current-owner body))))))

(deftest claim-unowned-draftset-submitted-by-self
  (let [draftset-location (create-draftset-through-api test-editor)]
    (submit-draftset-through-api test-editor draftset-location :publisher)
    (claim-draftset-through-api draftset-location test-editor)))

(deftest claim-owned-by-other-user-draftset-submitted-by-self
  (let [draftset-location (create-draftset-through-api test-editor)]
    (submit-draftset-through-api test-editor draftset-location :publisher)
    (claim-draftset-through-api draftset-location test-publisher)

    (let [response (route (create-claim-request draftset-location test-editor))]
      (assert-is-forbidden-response response))))

(deftest claim-draftset-owned-by-other-user
  (let [draftset-location (create-draftset-through-api test-editor)
        claim-request (create-claim-request draftset-location test-publisher)
        claim-response (route claim-request)]
    (assert-is-forbidden-response claim-response)))

(deftest claim-draftset-by-user-not-in-role
  (let [other-editor (user/create-user "edtheduck@example.com" :editor (user/get-digest test-password))
        draftset-location (create-draftset-through-api test-editor)]
    (memrepo/add-user *user-repo* other-editor)
    (submit-draftset-through-api test-editor draftset-location :publisher)
    (let [claim-response (route (create-claim-request draftset-location other-editor))]
      (assert-is-forbidden-response claim-response))))

(deftest claim-non-existent-draftset
  (let [claim-response (route (create-claim-request "/v1/draftset/missing" test-publisher))]
    (assert-is-not-found-response claim-response)))

(deftest get-options-test
  (let [draftset-location (create-draftset-through-api test-editor)
        options-request (with-identity test-editor {:uri draftset-location :request-method :options})
        {:keys [body] :as options-response} (route options-request)]
    (assert-is-ok-response options-response)
    (is (= #{:edit :delete :submit :claim} (set body)))))

(deftest get-options-for-non-existent-draftset
  (let [response (route (with-identity test-manager {:uri "/v1/draftset/missing" :request-method :options}))]
    (assert-is-not-found-response response)))

(defn- revert-draftset-graph-changes-request [draftset-location user graph]
  (with-identity user {:uri (str draftset-location "/changes") :request-method :delete :params {:graph graph}}))

(defn- revert-draftset-graph-changes-through-api [draftset-location user graph]
  (let [{:keys [body] :as response} (route (revert-draftset-graph-changes-request draftset-location user graph))]
    (assert-is-ok-response response)
    (assert-schema Draftset body)
    body))

(deftest revert-graph-change-in-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location live-graph)

    (let [{:keys [changes]} (get-draftset-info-through-api draftset-location test-editor)]
      (is (= #{live-graph} (key-set changes))))

    (let [{:keys [changes] :as ds-info} (revert-draftset-graph-changes-through-api draftset-location test-editor live-graph)]
      (is (= #{} (key-set changes))))

    (let [ds-quads (get-draftset-quads-through-api draftset-location test-editor "true")]
      (is (= (set (eval-statements quads)) (set ds-quads))))))

(deftest revert-graph-change-in-unowned-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location live-graph)

    (let [revert-request (revert-draftset-graph-changes-request draftset-location test-publisher live-graph)
          response (route revert-request)]
      (assert-is-forbidden-response response))))

(deftest revert-graph-change-in-draftset-unauthorised
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (delete-draftset-graph-through-api test-editor draftset-location live-graph)

    (let [revert-request {:uri (str draftset-location "/changes") :request-method :delete :params {:graph live-graph}}
          response (route revert-request)]
      (assert-is-unauthorised-response response))))

(deftest revert-non-existent-graph-change-in-draftest
  (let [draftset-location (create-draftset-through-api test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "http://missing")
        response (route revert-request)]
    (assert-is-not-found-response response)))

(deftest revert-change-in-non-existent-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (publish-quads-through-api quads)
    (let [revert-request (revert-draftset-graph-changes-request "/v1/draftset/missing" test-manager live-graph)
          response (route revert-request)]
      (assert-is-not-found-response response))))

(deftest revert-graph-change-request-without-graph-parameter
  (let [draftset-location (create-draftset-through-api test-editor)
        revert-request (revert-draftset-graph-changes-request draftset-location test-editor "tmp")
        revert-request (update-in revert-request [:params] dissoc :graph)
        response (route revert-request)]
    (assert-is-unprocessable-response response)))

(defn- copy-live-graph-into-draftset-request [draftset-location user live-graph]
  (with-identity user
    {:uri (str draftset-location "/graph") :request-method :put :params {:graph live-graph}}))

(defn- copy-live-graph-into-draftset [draftset-location user live-graph]
  (let [request (copy-live-graph-into-draftset-request draftset-location user live-graph)
        response (route request)]
    (await-success finished-jobs (:finished-job (:body response)))))

(deftest copy-live-graph-into-draftset-test
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))
        draftset-location (create-draftset-through-api test-editor)]
    (publish-quads-through-api quads)
    (copy-live-graph-into-draftset draftset-location test-editor live-graph)

    (let [ds-quads (get-draftset-quads-through-api draftset-location test-editor "false")]
      (is (= (set (eval-statements quads)) (set ds-quads))))))

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
      (assert-is-forbidden-response copy-response))))

(deftest copy-non-existent-live-graph
  (let [draftset-location (create-draftset-through-api test-editor)
        copy-request (copy-live-graph-into-draftset-request draftset-location test-editor "http://missing")
        copy-response (route copy-request)]
    (assert-is-unprocessable-response copy-response)))

(deftest copy-live-graph-into-non-existent-draftset
  (let [[live-graph quads] (first (group-by context (statements "test/resources/test-draftset.trig")))]
    (publish-quads-through-api quads)
    (let [copy-request (copy-live-graph-into-draftset-request "/v1/draftset/missing" test-publisher live-graph)
          copy-response (route copy-request)]
      (assert-is-not-found-response copy-response))))

(defn- get-users-request [user]
  (with-identity user {:uri "/v1/users" :request-method :get}))

(deftest get-users
  (let [users (user-repo/get-all-users *user-repo*)
        expected-summaries (map user/get-summary users)
        {:keys [body] :as response} (route (get-users-request test-editor))]
    (assert-is-ok-response response)
    (is (= (set expected-summaries) (set body)))))

(deftest get-users-unauthenticated
  (let [response (route {:uri "/v1/users" :request-method :get})]
    (assert-is-unauthorised-response response)))

(defn- setup-route [test-function]
  (let [users (memrepo/create-repository* test-editor test-publisher test-manager)]
    (binding [*user-repo* users
              *route* (draftset-api-routes *test-backend* users "Test")]
      (test-function))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each setup-route)
(use-fixtures :each (fn [tf]
                      (wrap-clean-test-db #(setup-route tf))))
