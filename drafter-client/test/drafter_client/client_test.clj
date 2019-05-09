(ns drafter-client.client-test
  (:require [clojure.spec.test.alpha :as st]
            [clojure.test :as t]
            [drafter-client.client :as sut]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.test-util.auth :as auth-util]
            [drafter-client.test-util.db :as db-util]
            [environ.core :refer [env]]
            [grafter-2.rdf4j.io :as rio]
            [grafter-2.rdf4j.repository :as gr-repo]
            [integrant.core :as ig]
            [martian.core :as martian])
  (:import java.net.URI))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Setup ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn with-spec-instrumentation [f]
  (try
    (st/instrument)
    (f)
    (finally
      (st/unstrument))))

(defn db-fixture [f]
  (let [stardog-query (env :sparql-query-endpoint)
        stardog-update (env :sparql-update-endpoint)]
    (assert stardog-query "Set SPARQL_QUERY_ENDPOINT to run these tests.")
    (assert stardog-update "Set SPARQL_UPDATE_ENDPOINT to run these tests.")
    (let [stardog-repo (gr-repo/sparql-repo stardog-query stardog-update)]
      (when-not (Boolean/parseBoolean (env :disable-drafter-cleaning-protection))
        (db-util/assert-empty stardog-repo))
      (f)
      (db-util/drop-all! stardog-repo))))

(defn drafter-client []
  (let [drafter-endpoint (env :drafter-endpoint)]
    (assert drafter-endpoint "Set DRAFTER_ENDPOINT to run these tests.")
    (sut/create drafter-endpoint
                :jws-key (env :drafter-jws-signing-key)
                :batch-size 150000)))

(clojure.pprint/pprint
 (:martian
  (sut/create (env :drafter-endpoint)
              :batch-size 150000
              :token-endpoint "https://dev-kkt-m758.eu.auth0.com/oauth/token"
              :client-id ""
              :client-secret ""
              )))

(defn test-triples []
  (let [file "./test/specific_mappingbased_properties_bg.nt"]
    (rio/statements file)))

(t/use-fixtures :each
  ;; Drop db after tests
  with-spec-instrumentation
  db-fixture)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/deftest live-repo-tests
  (let [client (drafter-client)
        repo (sut/->repo client auth-util/system-user sut/live)]
    (t/testing "Live database is empty"
      (with-open [conn (gr-repo/->connection repo)]
        (let [result (gr-repo/query conn "ASK WHERE { ?s ?p ?o }")]
          (t/is (false? result)
                "I really expected the database to be empty"))))))


(t/deftest draftsets-tests
  (let [client (drafter-client)
        user auth-util/system-user
        name "test-name"
        description "test-description"]
    (t/testing "Assumption test"
      (let [result (sut/draftsets client user)]
        (t/is (empty? result) "There should be no drafts")))
    (t/testing "Adding a draft set"
      (let [result (sut/new-draftset client user name description)]
        (t/is (= (draftset/name result) name))
        (t/is (= (draftset/description  result) description))))
    (t/testing "Reading back the draftset"
      (let [result (sut/draftsets client user)]
        (t/is (= 1 (count result)) "There should be one draft")
        (t/is (= (draftset/name (first result)) name))
        (t/is (= (draftset/description  (first result)) description))))
    (t/testing "Reading back the draftset"
      (let [[draftset] (sut/draftsets client user)
            async-delete-result (sut/remove-draftset client user draftset)
            finished-result (sut/resolve-job client user async-delete-result)
            all-draftsets (sut/draftsets client user)]
        (t/is (= "ok" (:type async-delete-result)))
        (t/is (= :drafter-client.client/completed finished-result))
        (t/is (empty? all-draftsets))))))

(t/deftest adding-to-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        user auth-util/system-user
        name "Draftset adding"
        description "Testing adding things, and reading them"
        draftset (sut/new-draftset client user name description)]
    (t/testing "Adding quads to a draft set"
      (let [graph (URI. "http://test.graph.com/quad-graph")
            quads (map #(assoc % :c graph) triples)
            quads (take how-many quads)
            job (sut/add client user draftset quads)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client user job)))
            quads* (sut/get client user draftset)]
        (t/is (= (set quads) (set quads*)))))
    (t/testing "Adding triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            triples (take how-many triples)
            job (sut/add client user draftset graph triples)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client user job)))
            triples* (sut/get client user draftset graph)]
        (t/is (= (set triples) (set triples*)))))))

(t/deftest adding-everything-to-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        expected-count (count triples)
        user auth-util/system-user
        name "Draftset adding"
        description "Testing adding things, and reading them"
        draftset (sut/new-draftset client user name description)]
    (t/testing "Adding quads to a draft set"
      (let [graph (URI. "http://test.graph.com/quad-graph")
            quads (map #(assoc % :c graph) triples)
            job (sut/add client user draftset quads)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client user job)))
            quads* (sut/get client user draftset)]
        (t/is expected-count (count quads*))))
    (t/testing "Adding triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            job (sut/add client user draftset graph triples)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client user job)))
            triples* (sut/get client user draftset graph)]
        (t/is (= expected-count (count triples*)))))))

(t/deftest adding-quads-to-multiple-graphs-in-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        user auth-util/system-user
        name "Draftset adding"
        description "Testing adding things, and reading them"
        draftset (sut/new-draftset client user name description)]
    (t/testing "Adding quads to a draft set"
      (let [graph-1 (URI. "http://test.graph.com/quad-graph1")
            quads-1 (map #(assoc % :c graph-1) triples)
            quads-1 (take how-many quads-1)
            job-1 (sut/add client user draftset quads-1)
            graph-2 (URI. "http://test.graph.com/quad-graph2")
            quads-2 (map #(assoc % :c graph-2) triples)
            quads-2 (take how-many (drop how-many quads-2))
            job-2 (sut/add client user draftset quads-2)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client user job-2)))
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client user job-2)))
            quads* (sut/get client user draftset)]
        (t/is (= (set (concat quads-1 quads-2)) (set quads*)))))))

(t/deftest adding-quads-to-multiple-graphs-in-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        user auth-util/system-user
        name "Draftset adding"
        description "Testing adding things, and reading them"
        draftset (sut/new-draftset client user name description)]
    (t/testing "Adding triples to a draft set"
      (let [graph-1 (URI. "http://test.graph.com/triple-graph1")
            triples-1 (take how-many triples)
            job-1 (sut/add client user draftset graph-1 triples-1)
            graph-2 (URI. "http://test.graph.com/triple-graph2")
            triples-2 (take how-many (drop how-many triples))
            job-2 (sut/add client user draftset graph-2 triples-2)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client user job-1)))
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client user job-2)))
            triples-1* (sut/get client user draftset graph-1)
            triples-2* (sut/get client user draftset graph-2)]
        (t/is (= (set triples-1) (set triples-1*)))
        (t/is (= (set triples-2) (set triples-2*)))))))

(t/deftest job-status
  (let [client (drafter-client)
        graph (URI. "http://test.graph.com/3")
        triples (test-triples)
        quads (map #(assoc % :c graph) triples)
        user auth-util/system-user
        name "Job status test"
        description "Job async response handling tests"
        draftset (sut/new-draftset client user name description)]
    (t/testing "Adding quads to a draft set"
      (let [async-job (sut/add client user draftset (take 5 quads))]
        (t/is (= "ok" (:type async-job)))
        (t/is (not (sut/job-complete? async-job)))
        (let [job-status (sut/resolve-job client user async-job)]
          (t/is (= :drafter-client.client/completed job-status)))))))


(t/deftest querying
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        user auth-util/system-user
        name "Draftset querying"
        description "Testing adding things, and querying them"
        draftset (sut/new-draftset client user name description)]
    (t/testing "Adding triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph1")
            triples (take how-many triples)
            job (sut/add client user draftset graph triples)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client user job)))
            repo (sut/->repo client user draftset)]
        (with-open [conn (gr-repo/->connection repo)]
          (let [res (gr-repo/query conn "ASK WHERE { ?s ?p ?o }")]
            (t/is (true? res))))))))


(t/deftest integrant-null-client
  (t/testing "missing a drafter uri or jws key returns nil client"
    (t/is (nil? (ig/init-key :drafter-client/client {})))
    (t/is (nil? (ig/init-key :drafter-client/client {:jws-key ""})))
    (t/is (nil? (ig/init-key :drafter-client/client {:drafter-uri ""})))
    (t/is (some? (ig/init-key :drafter-client/client
                              {:batch-size 10
                               :drafter-uri (env :drafter-endpoint)
                               :jws-key ""})))))
