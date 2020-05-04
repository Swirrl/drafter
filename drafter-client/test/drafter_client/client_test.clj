(ns drafter-client.client-test
  (:require [clojure.spec.test.alpha :as st]
            [clojure.test :as t :refer :all]
            [drafter.main :as main]
            [drafter.middleware.auth0-auth]
            [drafter.middleware.auth]
            [drafter-client.client :as sut]
            [drafter-client.client-spec :as spec]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.test-util.auth :as auth-util]
            [drafter-client.test-util.db :as db-util]
            [drafter-client.test-util.jwt :as jwt]
            [environ.core :refer [env]]
            [grafter-2.rdf4j.io :as rio]
            [grafter-2.rdf4j.repository :as gr-repo]
            [integrant.core :as ig]
            [clojure.java.io :as io]
            [grafter-2.rdf.protocols :as pr]
            [clojure.spec.alpha :as s]
            [clj-time.core :as time])
  (:import clojure.lang.ExceptionInfo
           java.net.URI
           [java.util UUID]
           (java.util.concurrent ExecutionException)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Setup ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Override the :drafter.auth.auth0/jwk init-key otherwise it'll be trying to
;; contact auth0
(defmethod ig/init-key :drafter.auth.auth0/jwk [_ {:keys [endpoint] :as opts}]
  (jwt/mock-jwk))

;; But this is the one that everything should use anyway
(defmethod ig/init-key :drafter.auth.auth0/mock-jwk [_ {:keys [endpoint] :as opts}]
  (jwt/mock-jwk))

(defn with-spec-instrumentation [f]
  (try
    (st/instrument)
    (f)
    (catch Throwable e
      (prn e)
      (throw e))
    (finally
      (st/unstrument))))

(defn- get-stardog-repo []
  (let [stardog-query (env :sparql-query-endpoint)
        stardog-update (env :sparql-update-endpoint)]
    (assert stardog-query "Set SPARQL_QUERY_ENDPOINT to run these tests.")
    (assert stardog-update "Set SPARQL_UPDATE_ENDPOINT to run these tests.")
    (gr-repo/sparql-repo "http://localhost:5820/drafter-client-test/query" "http://localhost:5820/drafter-client-test/update")))

(defn db-fixture [f]
  (let [stardog-repo (get-stardog-repo)]
    (when-not (Boolean/parseBoolean (env :disable-drafter-cleaning-protection))
      (db-util/assert-empty stardog-repo))
    (f)
    (db-util/drop-all! stardog-repo)))

(defn res-file [filename]
  (or (some-> filename io/resource io/file .getCanonicalPath)
      (throw (Exception. (format "Cannot find %s on resource path" filename)))))

(defn start-drafter-server []
  (main/-main (res-file "drafter-client-test-config.edn")
              (res-file "stasher-off.edn")))

(defn drafter-server-fixture [f]
  (try
    (start-drafter-server)
    (f)
    (finally
      (main/stop-system!))))

(defn drafter-client []
  (let [drafter-endpoint (env :drafter-endpoint)]
    (assert drafter-endpoint "Set DRAFTER_ENDPOINT to run these tests.")
    (sut/client drafter-endpoint
                :auth0-endpoint (env :auth0-domain)
                :batch-size 150000)))

(def triples-nt-filename "resources/specific_mappingbased_properties_bg.nt")

(defn test-triples []
  (let [file (res-file triples-nt-filename)]
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

(t/use-fixtures :each
  with-spec-instrumentation
  ;; Drop db after tests
  db-fixture)

(t/use-fixtures :once
  drafter-server-fixture)

(defn mock-job-with-kvs [& kvs]
  (let [job {:id (UUID/randomUUID)
             :user-id "abc@def.ghi"
             :status :pending
             :priority :batch-write
             :start-time (time/now)
             :finish-time (time/now)}]
    (apply assoc job kvs)))

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
            quads (map #(assoc % :c graph) (drop 97 triples))
            _ (sut/add-sync client token draftset quads)
            quads* (sut/get client token draftset)]
        (t/is (= (set quads) (set quads*)))))

    (t/testing "Adding input-stream of triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            baos (java.io.ByteArrayOutputStream. 8192)
            __ (pr/add (rio/rdf-writer baos :format :nt) triples)
            bais (java.io.ByteArrayInputStream. (.toByteArray baos))
            draftset (sut/new-draftset client token name description)
            _ (sut/add-sync client token draftset graph bais)
            quads (sut/get client token draftset)]
        (t/is (= (set (map #(assoc % :c graph) triples))
                 (set quads)))))

    (t/testing "Adding input-stream of quads to a draft set"
      (let [graph (URI. "http://test.graph.com/sexy-stream-of-quads-graph")
            quads (map #(assoc % :c graph) triples)
            baos (java.io.ByteArrayOutputStream. 8192)
            __ (pr/add (rio/rdf-writer baos :format :nq) quads)
            bais (java.io.ByteArrayInputStream. (.toByteArray baos))
            draftset (sut/new-draftset client token name description)
            _ (sut/add-sync client token draftset bais)
            quads* (sut/get client token draftset)]
        (t/is (= (set quads) (set quads*)))))

    (t/testing "Adding .nt file of triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            draftset (sut/new-draftset client token name description)
            _ (sut/add-sync client token draftset graph (io/file (res-file triples-nt-filename)))
            triples* (sut/get client token draftset)]
        (t/is (= 2252 (count triples*)))
        (t/is (= (set (map #(assoc % :c graph) (test-triples)))
                 (set triples*)))))

    (t/testing "Adding .ttl file of statements to a draft set"
      ;; check that the correct mime-type for the file is sent to Drafter
      (let [graph (URI. "http://test.graph.com/rdf-graph")
            file (res-file "resources/rdf-syntax-ns.ttl")
            draftset (sut/new-draftset client token name description)
            _ (sut/add-sync client token draftset graph (io/file file))
            triples* (sut/get client token draftset)]
        (t/is (= 102 (count triples*)))
        (t/is (= (set (map #(assoc % :c graph) (rio/statements file)))
                 (set triples*)))))

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
            quads* (sut/get client token draftset)]
        (t/is (= (set quads) (set quads*)))))

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
            quads* (sut/get client token draftset)]
        (is (= (set (concat quads-1 quads-2)) (set quads*)))))))

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
          (let [quads* (map #(assoc % :c graph) (gr-repo/query conn "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"))]
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
            quads* (sut/get client token draftset-2)]
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
          (sut/delete-graph client token draftset-2 graph-1)
          (sut/publish-sync client token draftset-2))

        (with-open [conn (-> client (sut/->repo token sut/live) (gr-repo/->connection))]
          (let [query (format "CONSTRUCT { ?s ?p ?o } WHERE { graph <%s> { ?s ?p ?o } }" (str graph-1))
                triples-1* (gr-repo/query conn query)]
            (is (empty? triples-1*)))
          (let [query (format "CONSTRUCT { ?s ?p ?o } WHERE { graph <%s> { ?s ?p ?o } }" (str graph-2))
                triples-2* (gr-repo/query conn query)]
            (is (= (set triples-2) (set triples-2*)))))))))

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
           (= (sut/job-timeout-exception? e))))))))

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
