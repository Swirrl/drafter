(ns drafter-client.client-test
  (:require
    [clj-time.core :as time]
    [clojure.java.io :as io]
    [clojure.test :as t :refer :all]
    [drafter-client.client :as sut]
    [drafter-client.client-spec]
    [drafter-client.client.draftset :as draftset]
    [drafter-client.client.endpoint :as endpoint]
    [drafter-client.test-helpers :as h]
    [drafter-client.test-util.auth :as auth-util]
    [drafter.main :as drafter]
    [drafter.test-common :refer [mock-jwk] :as tc]
    [drafter.util :as util]
    [environ.core :refer [env]]
    [grafter-2.rdf.protocols :as pr]
    [grafter-2.rdf4j.io :as rio]
    [grafter-2.rdf4j.repository :as gr-repo]
    [integrant.core :as ig])
  (:import clojure.lang.ExceptionInfo
           java.net.URI
           [java.util UUID]
           [java.util.concurrent ExecutionException]
           [java.util.zip GZIPOutputStream]
           [java.io File]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Setup ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Override the :drafter.auth.auth0/jwk init-key otherwise it'll be trying to
;; contact auth0
(defmethod ig/init-key :drafter.auth.auth0/jwk [_ {:keys [endpoint] :as opts}]
  (mock-jwk))

;; But this is the one that everything should use anyway
(defmethod ig/init-key :drafter.auth.auth0/mock-jwk [_ {:keys [endpoint] :as opts}]
  (mock-jwk))

(defn start-auth0-drafter-server []
  (drafter/-main (h/res-file "auth0-test-config.edn")
                 (h/res-file "stasher-off.edn")))

(defn drafter-server-fixture [f]
  (try
    (h/drop-test-db!)
    (start-auth0-drafter-server)
    (f)
    (finally
      (h/stop-drafter-server))))

(t/use-fixtures :each
  h/with-spec-instrumentation
  ;; Drop db after tests
  h/db-fixture)

(t/use-fixtures :once
  drafter-server-fixture)

(defn drafter-client []
  (let [drafter-endpoint (env :drafter-endpoint)]
    (assert drafter-endpoint "Set DRAFTER_ENDPOINT to run these tests.")
    (sut/client drafter-endpoint
                :auth0-endpoint (env :auth0-domain)
                :batch-size 150000)))

(defn test-triples []
  (let [file (h/res-file h/test-triples-filename)]
    (rio/statements file)))

(defn infinite-test-triples
  "Generate an infinite amount of test triples.  Take the amount you
  need!"
  [graph]
  (->> (range)
       (map (fn [i]
              (pr/->Quad (URI. (str "http://s/" i))
                         (URI. (str "http://p/" i))
                         i
                         graph)))))

(defn mock-job-with-kvs [& kvs]
  (let [job {:id (UUID/randomUUID)
             :user-id "abc@def.ghi"
             :status :pending
             :priority :batch-write
             :start-time (time/now)
             :finish-time (time/now)}]
    (apply assoc job kvs)))

(defn- write-gzipped-file [source dest]
  (with-open [gos (GZIPOutputStream. (io/output-stream dest))
              is (io/input-stream source)]
    (io/copy is gos)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/deftest job-status-test
  (let [job-id (UUID/randomUUID)
        restart-id (UUID/randomUUID)
        job (sut/->AsyncJob job-id restart-id)]
    (t/testing "Job succeeded"
      (let [state (mock-job-with-kvs :status :complete)
            status (sut/job-status job state)]
        (is (= state status))))
    (t/testing "Job failed"
      (let [details {:some "details"}
            state (mock-job-with-kvs
                   :status :complete
                   :error {:message ":(" :error-class "java.lang.Exception" :details details})
            status (sut/job-status job state)]
        (is (sut/job-failure-result? status))))
    (t/testing "In progress"
      (let [state (mock-job-with-kvs :status :pending)]
        (is (= ::sut/pending (sut/job-status job state)))))))

(t/deftest live-repo-tests
  (let [client (drafter-client)
        repo (sut/->repo client (auth-util/system-token) sut/live)]
    (t/testing "Live database is empty"
      (with-open [conn (gr-repo/->connection repo)]
        (let [result (gr-repo/query conn "ASK WHERE { ?s ?p ?o }")]
          (is (false? result)
              "I really expected the database to be empty"))))))

(t/deftest endpoints-unauthenticated-tests
  (let [client (drafter-client)]
    (t/testing "Public endpoint"
      (t/testing "default"
        (let [endpoints (sut/endpoints client nil)]
          (t/is (= 1 (count endpoints)))
          (t/is (endpoint/public-ref? (first endpoints)))))

      (t/testing "owned"
        (let [owned-endpoints (sut/endpoints client nil :owned)]
          (is (empty? owned-endpoints)))))

    (t/testing "With draftset"
      (let [token (auth-util/publisher-token)]
        (sut/new-draftset client token "name" "description")
        (let [endpoints (sut/endpoints client nil)]
          (t/is (= 1 (count endpoints)))
          (t/is (endpoint/public-ref? (first endpoints))))))))

(t/deftest endpoints-authenticated-tests
  (let [client (drafter-client)
        token (auth-util/publisher-token)]
    (t/testing "Public endpoint"
      (let [endpoints (sut/endpoints client token)]
        (is (= 1 (count endpoints)))
        (is (endpoint/public-ref? (first endpoints)))))

    (t/testing "With draftset"
      (let [ds (sut/new-draftset client token "name" "description")]
        (t/testing "All endpoints"
          (let [endpoints (sut/endpoints client token)]
            (t/is (= 2 (count endpoints)))))

        (t/testing "Owned endpoints"
          (let [owned-endpoints (sut/endpoints client token :owned)]
            (t/is (= 1 (count owned-endpoints)))
            (t/is (= (draftset/id ds) (draftset/id (first owned-endpoints))))))))))

(defn- update-public-endpoint! [client]
  (let [token (auth-util/publisher-token)
        ds (sut/new-draftset client token "temp" "temp")]
    (sut/wait! client token (sut/publish client token ds))))

(t/deftest get-endpoint-test
  (let [client (drafter-client)]
    (t/testing "Public endpoint"
      (let [endpoint (sut/get-public-endpoint client)]
        (is (endpoint/public-ref? endpoint))))

    (t/testing "Draftset"
      (let [token (auth-util/publisher-token)
            ds (sut/new-draftset client token "name" "description")]
        (t/testing "union with live"
          (update-public-endpoint! client)
          (let [endpoint (sut/get-endpoint client token ds {:union-with-live true})
                public-endpoint (sut/get-public-endpoint client)]
            (t/is (= (endpoint/endpoint-id ds) (endpoint/endpoint-id endpoint)))
            (t/is (= (endpoint/updated-at public-endpoint)
                     (endpoint/updated-at endpoint)))
            (t/is (= (util/merge-versions (endpoint/version ds)
                                          (endpoint/version public-endpoint))
                     (endpoint/version endpoint)))))

        (t/testing "default"
          (let [endpoint (sut/get-endpoint client token ds)]
            (t/is (= (endpoint/endpoint-id ds) (endpoint/endpoint-id endpoint)))))))))

(t/deftest draftsets-tests
  (let [client (drafter-client)
        token (auth-util/system-token)
        name "test-name"
        description "test-description"]
    (t/testing "Assumption test"
      (let [result (sut/draftsets client token)]
        (is (empty? result) "There should be no drafts")))
    (t/testing "Adding a draft set"
      (let [result (sut/new-draftset client token name description)]
        (is (= (draftset/name result) name))
        (is (= (draftset/description  result) description))))
    (t/testing "Reading back the draftset"
      (let [result (sut/draftsets client token)]
        (is (= 1 (count result)) "There should be one draft")
        (is (= (draftset/name (first result)) name))
        (is (= (draftset/description  (first result)) description))))
    (t/testing "Reading back the draftset"
      (let [[draftset] (sut/draftsets client token)]
        (sut/remove-draftset-sync client token draftset)
        (let [all-draftsets (sut/draftsets client token)]
          (t/is (empty? all-draftsets)))))))

(t/deftest draftsets-include-test
  (let [client (drafter-client)
        publisher-token (auth-util/publisher-token)
        editor-token (auth-util/editor-token)
        ds-1 (sut/new-draftset client publisher-token "first" "description")
        ds-2 (sut/new-draftset client publisher-token "second" "description")
        ds-3 (sut/new-draftset client editor-token "third" "description")]
    (sut/submit-to-permission client publisher-token (draftset/id ds-2) :drafter:draft:claim)
    (sut/share-with-permission client editor-token (draftset/id ds-3) :drafter:draft:view)
    (t/testing "default"
      (let [draftsets (sut/draftsets client publisher-token)]
        (t/is (= #{(draftset/id ds-1) (draftset/id ds-2) (draftset/id ds-3)}
                 (set (map draftset/id draftsets))))))
    (sut/unshare client editor-token (draftset/id ds-3))
    (t/testing "all"
      (let [draftsets (sut/draftsets client publisher-token {:include :all})]
        (t/is (= #{(draftset/id ds-1) (draftset/id ds-2)}
                 (set (map draftset/id draftsets))))))
    (t/testing "owned"
      (let [owned (sut/draftsets client publisher-token {:include :owned})]
        (is (= #{(draftset/id ds-1)}
               (set (map draftset/id owned))))))
    (t/testing "claimable"
      (let [claimable (sut/draftsets client publisher-token {:include :claimable})]
        (is (= #{(draftset/id ds-2)}
               (set (map draftset/id claimable))))))))

(t/deftest draftsets-union-with-live-test
  (let [client (drafter-client)
        token (auth-util/publisher-token)
        ds (sut/new-draftset client token "test" "description")]
    (update-public-endpoint! client)
    (t/testing "default"
      (let [draftsets (sut/draftsets client token)]
        (is (= 1 (count draftsets)))
        (is (= (draftset/id ds) (draftset/id (first draftsets))))
        (is (= (endpoint/updated-at ds)
               (endpoint/updated-at (first draftsets))))
        (is (= (endpoint/version ds)
               (endpoint/version (first draftsets))))))
    (t/testing "true"
      (let [draftsets (sut/draftsets client token {:union-with-live true})
            public-endpoint (sut/get-public-endpoint client)]
        (is (= 1 (count draftsets)))
        (is (= (draftset/id ds) (draftset/id (first draftsets))))
        (is (= (endpoint/updated-at public-endpoint)
               (endpoint/updated-at (first draftsets))))
        (is (= (util/merge-versions (endpoint/version ds)
                                    (endpoint/version public-endpoint))
               (endpoint/version (first draftsets))))))))

(t/deftest get-draftset-test
  (let [client (drafter-client)
        token (auth-util/publisher-token)
        ds (sut/new-draftset client token "test" "test")]
    (t/testing "default"
      (let [result (sut/get-draftset client token (draftset/id ds))]
        (is (= ds result))))

    (t/testing "union with live"
      (update-public-endpoint! client)
      (let [public-endpoint (sut/get-public-endpoint client)
            result (sut/get-draftset client token (draftset/id ds) {:union-with-live true})]
        (is (= (draftset/id ds) (draftset/id result)))
        (is (= (endpoint/updated-at public-endpoint)
               (endpoint/updated-at result)))
        (is (= (util/merge-versions (endpoint/version ds)
                                    (endpoint/version public-endpoint))
               (endpoint/version result)))))))

(t/deftest adding-to-a-draftset
  (let [client (drafter-client)
        token (auth-util/system-token)
        name "Draftset adding"
        description "Testing adding things, and reading them"
        triples (test-triples)]
    (t/testing "Adding triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            draftset (sut/new-draftset client token name description)
            _ (sut/add-sync client token draftset graph triples)
            triples* (sut/get client token draftset graph)]
        (t/is (= (set triples) (set triples*)))))

    (t/testing "Adding quads to a draft set"
      (let [graph (URI. "http://test.graph.com/vanilla-quad-graph")
            draftset (sut/new-draftset client token name description)
            quads (map #(assoc % :c graph) (take 97 triples))
            _ (sut/add-sync client token draftset quads)
            quads* (h/get-user-quads client token draftset)]
        (t/is (= (set quads) (set quads*)))))

    (t/testing "Adding input-stream of triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            baos (java.io.ByteArrayOutputStream. 8192)
            __ (pr/add (rio/rdf-writer baos :format :nt) triples)
            bais (java.io.ByteArrayInputStream. (.toByteArray baos))
            draftset (sut/new-draftset client token name description)
            _ (sut/add-sync client token draftset graph bais)
            quads (h/get-user-quads client token draftset)
            expected-quads (set (map #(assoc % :c graph) triples))]
        (t/is (= expected-quads (set quads)))))

    (t/testing "Adding input-stream of quads to a draft set"
      (let [graph (URI. "http://test.graph.com/sexy-stream-of-quads-graph")
            quads (map #(assoc % :c graph) triples)
            baos (java.io.ByteArrayOutputStream. 8192)
            __ (pr/add (rio/rdf-writer baos :format :nq) quads)
            bais (java.io.ByteArrayInputStream. (.toByteArray baos))
            draftset (sut/new-draftset client token name description)
            _ (sut/add-sync client token draftset bais)
            quads* (h/get-user-quads client token draftset)]
        (t/is (= (set quads) (set quads*)))))

    (t/testing "Adding .nt file of triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            draftset (sut/new-draftset client token name description)
            _ (sut/add-sync client token draftset graph (io/file (h/res-file h/test-triples-filename)))
            triples* (sut/get client token draftset graph)]
        (t/is (= 2252 (count triples*)))
        (t/is (= (set (test-triples)) (set triples*)))))

    (t/testing "Adding .ttl file of statements to a draft set"
      ;; check that the correct mime-type for the file is sent to Drafter
      (let [graph (URI. "http://test.graph.com/rdf-graph")
            file (h/res-file "resources/rdf-syntax-ns.ttl")
            draftset (sut/new-draftset client token name description)
            _ (sut/add-sync client token draftset graph (io/file file))
            triples* (sut/get client token draftset graph)]
        (t/is (= 102 (count triples*)))
        (t/is (= (set (rio/statements file)) (set triples*)))))

    (t/testing "Adding invalid quads to a draft set"
      (let [draftset (sut/new-draftset client token name description)
            quads [(pr/->Quad "some" :invalid "quad" nil)
                   (pr/->Quad (URI. "http://x.com/s") (URI. "http://x.com/p") (URI. "http://x.com/o") nil)]]
        (is (thrown-with-msg? ExecutionException #"It looks like you have an incorrect data type inside a quad"
                              (sut/add client token draftset quads)))))

    (t/testing "Adding triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            draftset (sut/new-draftset client token name description)
            _ (sut/add-sync client token draftset graph triples)
            triples* (sut/get client token draftset graph)]
        (t/is (= (set triples) (set triples*)))))))

(deftest adding-with-add-data-when-writes-rejected
  (let [client (drafter-client)
        token (auth-util/system-token)
        name "Draftset adding when jobs and writes are rejected"
        description "Testing adding things, and reading them"
        triples (test-triples)]

    (testing "Adding triples to a draft set when writes are rejected"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            draftset (sut/new-draftset client token name description)]
        (try
          (tc/timeout 500 #(drafter.write-scheduler/toggle-reject-and-flush!))
          (is
            (thrown-with-msg?
              ExceptionInfo
              #"clj-http: status 503"
              (tc/assert-is-service-unavailable-response (sut/add-data-sync client token draftset triples {:graph graph})))
            "should receive a 503 unavailable response")
          (finally
            (tc/timeout 200 #(drafter.write-scheduler/toggle-reject-and-flush!))))))))

(deftest adding-with-add-data
  (let [client (drafter-client)
        token (auth-util/system-token)
        name "Draftset adding"
        description "Testing adding things, and reading them"
        triples (test-triples)]
    (testing "Adding triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            draftset (sut/new-draftset client token name description)
            _ (sut/add-data-sync client token draftset triples {:graph graph})
            triples* (sut/get client token draftset graph)]
        (t/is (= (set triples) (set triples*)))))

    (testing "Adding quads to a draft set"
      (let [graph (URI. "http://test.graph.com/vanilla-quad-graph")
            draftset (sut/new-draftset client token name description)
            quads (map #(assoc % :c graph) (drop 97 triples))
            _ (sut/add-data-sync client token draftset quads)
            quads* (h/get-user-quads client token draftset)]
        (t/is (= (set quads) (set quads*)))))

    (testing "Adding quads from a file to a draft set"
      (doseq [gzip? [false true]]
        (t/testing (format "with%s gzip" (if gzip? "" "out"))
          (let [f (io/file "test/resources/test_data.trig")
                draftset (sut/new-draftset client token name description)
                _ (sut/add-data-sync client token draftset f {:gzip gzip?})
                quads* (h/get-user-quads client token draftset)
                expected-quads (set (rio/statements f))]
            (t/is (= expected-quads (set quads*)))))))

    (t/testing "Add quads from a gzipped file"
      (let [source (io/file "test/resources/test_data.trig")
            expected-quads (set (rio/statements source))]
        (t/testing "with format and gzip extension"
          (let [f (File/createTempFile "drafter-client" ".trig.gz")]
            (try
              (write-gzipped-file source f)
              (let [draftset (sut/new-draftset client token name description)
                    _ (sut/add-data-sync client token draftset f)
                    quads* (set (h/get-user-quads client token draftset))]
                (t/is (= expected-quads quads*)))
              (finally
                (.delete f)))))

        (t/testing "with gzip extension and specified format"
          (let [f (File/createTempFile "drafter-client" ".gz")]
            (try
              (write-gzipped-file source f)
              (let [draftset (sut/new-draftset client token name description)
                    _ (sut/add-data-sync client token draftset f {:format :trig})
                    quads* (set (h/get-user-quads client token draftset))]
                (t/is (= expected-quads quads*)))
              (finally
                (.delete f)))))

        (t/testing "with unknown extension"
          (let [f (File/createTempFile "drafter-client" ".mysterious")]
            (try
              (write-gzipped-file source f)
              (let [draftset (sut/new-draftset client token name description)
                    _ (sut/add-data-sync client token draftset f {:format :trig :gzip :applied})
                    quads* (set (h/get-user-quads client token draftset))]
                (t/is (= expected-quads quads*)))
              (finally
                (.delete f)))))))

    (testing "Custom metadata gets passed on to job"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            draftset (sut/new-draftset client token name description)
            result (sut/add-data client token draftset triples {:graph graph
                                                                :metadata {:title "Custom job title"}})
            job (sut/job client token (:job-id result))]
        (is (= #{:title :draftset :operation} (-> job :metadata keys set)))
        (is (= "Custom job title" (-> job :metadata :title)))
        ;; wait for this so it doesn't interfere with subsequent tests
        (sut/wait! client token result)))

    (testing "it silently ignores metadata that's not an object"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            draftset (sut/new-draftset client token name description)
            result (sut/add-data client token draftset triples {:graph graph :metadata "not an object"})
            job (sut/job client token (:job-id result))]
        (is (= #{:draftset :operation} (-> job :metadata keys set)))
        (sut/wait! client token result)))))

(t/deftest deleting-from-a-draftset
  (let [client (drafter-client)
        token (auth-util/system-token)
        draftset (sut/new-draftset client token "my name" "my description")
        x-triple (pr/->Triple (URI. "http://x.com/s") (URI. "http://x.com/p") (URI. "http://x.com/o"))
        y-triple (pr/->Triple (URI. "http://y.com/s") (URI. "http://y.com/p") (URI. "http://y.com/o"))
        graph (URI. "http://test.graph.com/triple-graph")]
    (t/testing "Deleting triples from a draft set"
      (let [original-triples [x-triple y-triple]
            to-delete [x-triple]
            expected-triples [y-triple]
            _ (sut/add-sync client token draftset graph original-triples)
            _ (sut/delete-triples-sync client token draftset graph to-delete)
            triples* (sut/get client token draftset graph)]
        (t/is (= (set expected-triples) (set triples*)))))

    (t/testing "Deleting triples with metadata"
      (let [result (sut/delete-triples client token draftset graph [y-triple] {:metadata {:title "Custom job title"}})
            job (sut/job client token (:job-id result))]
        (is (= #{:title :draftset :operation} (-> job :metadata keys set)))
        (is (= "Custom job title" (-> job :metadata :title)))
        (sut/wait! client token result)))

    (t/testing "Deleting quads with metadata"
      (let [quad (pr/->Quad (URI. "http://x.com/s")
                            (URI. "http://x.com/p")
                            (URI. "http://x.com/o")
                            (URI. "http://x.com/g"))
            result (sut/delete-quads client token draftset [quad] {:metadata {:title "Custom job title"}})
            job (sut/job client token (:job-id result))]
        (is (= #{:title :draftset :operation} (-> job :metadata keys set)))
        (is (= "Custom job title" (-> job :metadata :title)))
        (sut/wait! client token result)))))

(t/deftest adding-quads-to-multiple-graphs-in-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        token (auth-util/system-token)
        name "Draftset adding"
        description "Testing adding things, and reading them"
        draftset (sut/new-draftset client token name description)]
    (t/testing "Adding quads to a draft set"
      (let [graph-1 (URI. "http://test.graph.com/quad-graph1")
            quads-1 (map #(assoc % :c graph-1) triples)
            quads-1 (take how-many quads-1)
            _ (sut/add-sync client token draftset quads-1)
            graph-2 (URI. "http://test.graph.com/quad-graph2")
            quads-2 (map #(assoc % :c graph-2) triples)
            quads-2 (take how-many (drop how-many quads-2))
            _ (sut/add-sync client token draftset quads-2)
            quads* (h/get-user-quads client token draftset)]
        (is (= (count (set (concat quads-1 quads-2))) (count (set quads*))))))))

(t/deftest adding-triples-to-multiple-graphs-in-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        token (auth-util/system-token)
        name "Draftset adding"
        description "Testing adding things, and reading them"
        draftset (sut/new-draftset client token name description)]
    (t/testing "Adding triples to a draft set"
      (let [graph-1 (URI. "http://test.graph.com/triple-graph1")
            triples-1 (take how-many triples)
            _ (sut/add-sync client token draftset graph-1 triples-1)
            graph-2 (URI. "http://test.graph.com/triple-graph2")
            triples-2 (take how-many (drop how-many triples))
            _ (sut/add-sync client token draftset graph-2 triples-2)
            triples-1* (sut/get client token draftset graph-1)
            triples-2* (sut/get client token draftset graph-2)]
        (is (= (set triples-1) (set triples-1*)))
        (is (= (set triples-2) (set triples-2*)))))))

(t/deftest publishing-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        token (auth-util/system-token)
        name "Draftset publishing"
        description "Testing adding things, publishing them, and reading them"
        draftset (sut/new-draftset client token name description)]
    (t/testing "Publishing a draft set"
      (let [graph (URI. "http://test.graph.com/quad-graph")
            quads (map #(assoc % :c graph) triples)
            quads (take how-many quads)]
        (sut/add-sync client token draftset quads)
        (sut/publish-sync client token draftset)
        (with-open [conn (-> client (sut/->repo token sut/live) (gr-repo/->connection))]
          (let [user-triples (h/query-user-triples conn)
                quads* (map #(assoc % :c graph) user-triples)]
            (is (= (set quads) (set quads*)))))))))

(t/deftest publishing-with-metadata
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        token (auth-util/system-token)
        name "Draftset publishing"
        description "Testing adding things, publishing them, and reading them"]
    (t/testing "Publishing a draft set"
      (let [graph (URI. "http://test.graph.com/quad-graph")
            quads (map #(assoc % :c graph) triples)
            quads (take how-many quads)
            draftset (sut/new-draftset client token name description)]
        (sut/add-sync client token draftset quads)

        (let [result (sut/publish client token draftset {:metadata {:title "Custom job title"
                                                                    :multiword-key "Key"
                                                                    :$-9%&*-> "weird key"}})
              job (sut/job client token (:job-id result))]
          (is (= #{:title :draftset :operation :multiword-key :$-9%&*->}
                 (-> job :metadata keys set)))
          (is (= "Custom job title" (-> job :metadata :title)))
          (sut/wait! client token result))))))

(t/deftest loading-a-graph-into-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        token (auth-util/system-token)
        name "Loading a graph into a draftset"
        description "Testing loading a graph into a draftset"
        draftset-1 (sut/new-draftset client token name description)]
    (t/testing "Publishing a draft set"
      (let [graph (URI. "http://test.graph.com/quad-graph")
            quads (map #(assoc % :c graph) triples)
            quads (take how-many quads)
            _ (sut/add-sync client token draftset-1 quads)
            _ (sut/publish-sync client token draftset-1)
            draftset-2 (sut/new-draftset client token name description)
            job-3 (sut/load-graph client token draftset-2 graph)
            _ (sut/wait! client token job-3)
            quads* (h/get-user-quads client token draftset-2)]
        (is (= (set quads) (set quads*)))))))

(t/deftest loading-a-graph-into-a-draftset-with-metadata
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        token (auth-util/system-token)
        name "Loading a graph into a draftset"
        description "Testing loading a graph into a draftset"
        draftset-1 (sut/new-draftset client token name description)]
    (t/testing "Publishing a draft set"
      (let [graph (URI. "http://test.graph.com/quad-graph")
            quads (map #(assoc % :c graph) triples)
            quads (take how-many quads)
            _ (sut/add-sync client token draftset-1 quads)
            _ (sut/publish-sync client token draftset-1)
            draftset-2 (sut/new-draftset client token name description)
            result (sut/load-graph client token draftset-2 graph {:metadata {:title "Custom job title"}})
            job (sut/job client token (:job-id result))]
        (is (= #{:title :draftset :operation} (-> job :metadata keys set)))
        (is (= "Custom job title" (-> job :metadata :title)))
        (sut/wait! client token result)))))

(t/deftest deleting-a-draftset
  (testing "Deleting a draftset with metadata"
    (let [client (drafter-client)
          token (auth-util/system-token)
          draftset (sut/new-draftset client token "Test" "Delete me")
          result (sut/remove-draftset client token draftset {:metadata {:title "Custom job title"}})
          job (sut/job client token (:job-id result))]
      (is (= #{:title :draftset :operation} (-> job :metadata keys set)))
      (is (= "Custom job title" (-> job :metadata :title)))
      (sut/wait! client token result))))

(t/deftest deleting-a-graph-from-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        token (auth-util/system-token)
        name "Draftset deleting"
        description "Testing deleting a graph from a draftset"
        draftset-1 (sut/new-draftset client token name description)]
    (t/testing "Deleting graph from live"
      (let [graph-1 (URI. "http://test.graph.com/triple-graph1")
            triples-1 (take how-many triples)
            graph-2 (URI. "http://test.graph.com/triple-graph2")
            triples-2 (take how-many (drop how-many triples))]
        (sut/add-sync client token draftset-1 graph-1 triples-1)
        (sut/add-sync client token draftset-1 graph-2 triples-2)
        (sut/publish-sync client token draftset-1)

        (let [draftset-2 (sut/new-draftset client token name description)]
          (sut/delete-graph-2-sync client token draftset-2 graph-1)
          (sut/publish-sync client token draftset-2))

        (with-open [conn (-> client (sut/->repo token sut/live) (gr-repo/->connection))]
          (let [query (format "CONSTRUCT { ?s ?p ?o } WHERE { graph <%s> { ?s ?p ?o } }" (str graph-1))
                triples-1* (gr-repo/query conn query)]
            (is (empty? triples-1*)))
          (let [query (format "CONSTRUCT { ?s ?p ?o } WHERE { graph <%s> { ?s ?p ?o } }" (str graph-2))
                triples-2* (gr-repo/query conn query)]
            (is (= (set triples-2) (set triples-2*)))))))))

(t/deftest deleting-a-graph-from-a-draftset-silently
  (let [client (drafter-client)
        token (auth-util/system-token)
        name "Draftset deleting"
        description "Testing deleting a graph from a draftset silently"
        draftset (sut/new-draftset client token name description)
        non-graph (URI. "http://test.graph.com/non-graph")]
    (t/testing "Can delete a graph that doesn't exist with :silent"
      (sut/delete-graph-2-sync client token draftset non-graph
        {:silent true}))
    (t/testing "Can't delete a graph that doesn't exist without :silent"
      (is (thrown-with-msg? ExceptionInfo #"status 422"
            (sut/delete-graph-2-sync client token draftset non-graph
              {:silent false}))))))

(t/deftest job-status
  (let [client (drafter-client)
        graph (URI. "http://test.graph.com/3")
        triples (test-triples)
        quads (map #(assoc % :c graph) triples)
        token (auth-util/system-token)
        name "Job status test"
        description "Job async response handling tests"
        draftset (sut/new-draftset client token name description)]
    (t/testing "Adding quads to a draft set"
      (let [async-job (sut/add client token draftset (take 5 quads))
            id (:job-id async-job)
            known-job (sut/job client token id)]
        (is known-job)
        (is (= :pending (:status known-job)))
        (is (contains? (set (map :id (sut/jobs client token)))
                       (:id known-job)))
        (sut/wait! client token async-job)
        (let [known-job' (sut/job client token id)]
          (is known-job')
          (is (= :complete (:status known-job')))
          (is (contains? (set (sut/jobs client token)) known-job')))))))

(t/deftest querying
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        token (auth-util/system-token)
        name "Draftset querying"
        description "Testing adding things, and querying them"
        draftset (sut/new-draftset client token name description)]
    (t/testing "Adding triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph1")
            triples (take how-many triples)
            repo (sut/->repo client token draftset)]
        (sut/add-sync client token draftset graph triples)
        (with-open [conn (gr-repo/->connection repo)]
          (let [res (gr-repo/query conn "ASK WHERE { ?s ?p ?o }")]
            (is (true? res))))))))

(defn test-client-job-timeout [client]
  (testing "remove-draftset-sync times out"
    (let [token (auth-util/system-token)
          name "Draftset querying"
          desc "Testing adding things, and querying them"
          draftset (sut/new-draftset client token name desc)]
      (is
       (try
         (sut/remove-draftset-sync client token draftset)
         false
         (catch ExceptionInfo e
           (sut/job-timeout-exception? e)))))))

(deftest client-job-timeout-test
  (let [job-timeout -1]
    ;;  ^^ this is a bit of a hack as we never expect a timeout to be negative
    ;; but seeing as the timeout will always happen when timeout <= 0 then we
    ;; can use this to simulate a timeout without having to get drafter to hang
    ;; for a bit.
    (testing "with-job-timeout applies job-timeout to existing client"
      (test-client-job-timeout  (sut/with-job-timeout (drafter-client) job-timeout)))
    (testing "client with :job-timeout opt applies timeout to new client"
      (test-client-job-timeout (ig/init-key :drafter-client/client
                                            {:drafter-uri (env :drafter-endpoint)
                                             :auth0-endpoint (env :auth0-domain)
                                             :batch-size 150000
                                             :job-timeout job-timeout})))))

(t/deftest integrant-null-client
  (t/testing "missing a drafter uri key returns nil client"
    (is (nil? (ig/init-key :drafter-client/client {})))
    (is (nil? (ig/init-key :drafter-client/client {:drafter-uri ""})))
    (is (some? (ig/init-key :drafter-client/client
                            {:batch-size 10
                             :drafter-uri (env :drafter-endpoint)
                             :auth0-endpoint (env :auth0-domain)})))))

(t/deftest writes-locked?-test
  (let [client (drafter-client)
        quads (infinite-test-triples (URI. "http://test.graph.com/quad-graph"))
        how-many 1000 ;; NOTE: this needs to be enough that the write lock
                      ;; engages long enough to test writes-locked?
        token (auth-util/system-token)
        name "Draftset publishing"
        description "Testing adding things, publishing them, and reading them"
        draftset-1 (sut/new-draftset client token name description)]
    (t/testing "That writes-locked? returns correct value"
      (is (false? (sut/writes-locked? client token)))
      (let [quads (take how-many quads)
            job-1 (sut/add-sync client token draftset-1 quads)
            job-2 (sut/publish client token draftset-1)]
        (is (true? (sut/writes-locked? client token)))
        (is (= :drafter-client.client/completed
               (sut/resolve-job client token job-2)))))))

(deftest wait-result!-test
  (let [client (drafter-client)
        quads (infinite-test-triples (URI. "http://test.graph.com/quad-graph"))
        how-many 1000
        token (auth-util/system-token)
        name "Draftset publishing"
        description "Testing adding things, publishing them, and reading them"
        draftset-1 (sut/new-draftset client token name description)
        quads (take how-many quads)]
    (t/testing "That wait-result! returns the :complete job"
      (let [job-1 (sut/add-data client token draftset-1 quads)
            res-1 (sut/wait-result! client token job-1)]
        (is (= (:job-id job-1) (:id res-1)))
        (is (= :complete (:status res-1)))))

    (t/testing "With timeouts"
      (let [timeout -1]
        (letfn [(wait-for-drafter-finish [job]
                  ;; As the job timeouts are a client side only feature
                  ;; to allow control to return and continue in the
                  ;; caller, they do not stop the underlying drafter
                  ;; job.  Hence after raising a client side timeout,
                  ;; this test needs to wait for drafter to finish
                  ;; processing the initial call to sut/add-data, in
                  ;; order to guarantee that the test data tear-down
                  ;; happens after we've added the data.
                  (sut/wait-result! (sut/with-job-timeout client ##Inf) token job))]
          (t/testing "set on client"
            (let [timeout-client (sut/with-job-timeout client timeout)]
              (t/testing "returns a job-timeout-exception"
                (let [job (sut/add-data client token draftset-1 quads)
                      res-1 (sut/wait-result! timeout-client token job)]
                  (is (sut/job-timeout-exception? res-1))
                  ;; wait-for-drafter-finish is only required because this test
                  ;; raises a timeout exception.
                  (wait-for-drafter-finish job)))))

          (t/testing "passed as a parameter"
            (t/testing "returns a job-timeout-exception"
              (let [job (sut/add-data client token draftset-1 quads)
                    res-1 (sut/wait-result! client token job {:job-timeout timeout})]

                (is (sut/job-timeout-exception? res-1))
                ;; wait-for-drafter-finish is only required because this test
                ;; raises a timeout exception.
                (wait-for-drafter-finish job)))))))

    (t/testing "That wait-result! returns the job-not-found-exception"
      (let [job-id (UUID/randomUUID)
            job (sut/->AsyncJob job-id (UUID/randomUUID))
            res-1 (sut/wait-result! client token job)]
        (is (instance? ExceptionInfo res-1))
        (is (= (:job-id (ex-data res-1)) job-id))
        (is (= (.getMessage res-1) "Job not found"))))))
