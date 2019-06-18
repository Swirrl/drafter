(ns drafter-client.client-test
  (:require [clj-http.client :as http]
            [clojure.spec.test.alpha :as st]
            [clojure.test :as t]
            [drafter.main :as main]
            [drafter.middleware.auth0-auth]
            [drafter.middleware.auth]
            [drafter-client.client :as sut]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.test-util.auth :as auth-util]
            [drafter-client.test-util.db :as db-util]
            [drafter-client.test-util.jwt :as jwt]
            [environ.core :refer [env]]
            [grafter-2.rdf4j.io :as rio]
            [grafter-2.rdf4j.repository :as gr-repo]
            [integrant.core :as ig]
            [martian.core :as martian])
  (:import java.net.URI))

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

(defn start-drafter-server []
  (main/-main "../drafter/env/dev/resources/drafter-dev-config.edn"
              "resources/drafter-mock-middleware.edn"))

(defn drafter-server-fixture [f]
  (try
    (start-drafter-server)
    (f)
    (finally
      (main/stop-system!))))

(defn drafter-client []
  (let [drafter-endpoint (env :drafter-endpoint)]
    (assert drafter-endpoint "Set DRAFTER_ENDPOINT to run these tests.")
    (sut/web-client drafter-endpoint
                    :auth0-endpoint (env :auth0-domain)
                    :batch-size 150000)))

(defn test-triples []
  (let [file "./test/specific_mappingbased_properties_bg.nt"]
    (rio/statements file)))

(t/use-fixtures :each
  with-spec-instrumentation
  ;; Drop db after tests
  db-fixture)

(t/use-fixtures :once
  drafter-server-fixture)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/deftest live-repo-tests
  (let [client (drafter-client)
        repo (sut/->repo client (auth-util/system-token) sut/live)]
    (t/testing "Live database is empty"
      (with-open [conn (gr-repo/->connection repo)]
        (let [result (gr-repo/query conn "ASK WHERE { ?s ?p ?o }")]
          (t/is (false? result)
                "I really expected the database to be empty"))))))


(t/deftest draftsets-tests
  (let [client (drafter-client)
        token (auth-util/system-token)
        name "test-name"
        description "test-description"]
    (t/testing "Assumption test"
      (let [result (sut/draftsets client token)]
        (t/is (empty? result) "There should be no drafts")))
    (t/testing "Adding a draft set"
      (let [result (sut/new-draftset client token name description)]
        (t/is (= (draftset/name result) name))
        (t/is (= (draftset/description  result) description))))
    (t/testing "Reading back the draftset"
      (let [result (sut/draftsets client token)]
        (t/is (= 1 (count result)) "There should be one draft")
        (t/is (= (draftset/name (first result)) name))
        (t/is (= (draftset/description  (first result)) description))))
    (t/testing "Reading back the draftset"
      (let [[draftset] (sut/draftsets client token)
            async-delete-result (sut/remove-draftset client token draftset)
            finished-result (sut/resolve-job client token async-delete-result)
            all-draftsets (sut/draftsets client token)]
        (t/is (= "ok" (:type async-delete-result)))
        (t/is (= :drafter-client.client/completed finished-result))
        (t/is (empty? all-draftsets))))))

(t/deftest adding-to-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        how-many 5
        token (auth-util/system-token)
        name "Draftset adding"
        description "Testing adding things, and reading them"
        draftset (sut/new-draftset client token name description)]
    (t/testing "Adding quads to a draft set"
      (let [graph (URI. "http://test.graph.com/quad-graph")
            quads (map #(assoc % :c graph) triples)
            quads (take how-many quads)
            job (sut/add client token draftset quads)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client token job)))
            quads* (sut/get client token draftset)]
        (t/is (= (set quads) (set quads*)))))
    (t/testing "Adding triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            triples (take how-many triples)
            job (sut/add client token draftset graph triples)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client token job)))
            triples* (sut/get client token draftset graph)]
        (t/is (= (set triples) (set triples*)))))))

(t/deftest adding-everything-to-a-draftset
  (let [client (drafter-client)
        triples (test-triples)
        expected-count (count triples)
        token (auth-util/system-token)
        name "Draftset adding"
        description "Testing adding things, and reading them"
        draftset (sut/new-draftset client token name description)]
    (t/testing "Adding quads to a draft set"
      (let [graph (URI. "http://test.graph.com/quad-graph")
            quads (map #(assoc % :c graph) triples)
            job (sut/add client token draftset quads)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client token job)))
            quads* (sut/get client token draftset)]
        (t/is expected-count (count quads*))))
    (t/testing "Adding triples to a draft set"
      (let [graph (URI. "http://test.graph.com/triple-graph")
            job (sut/add client token draftset graph triples)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client token job)))
            triples* (sut/get client token draftset graph)]
        (t/is (= expected-count (count triples*)))))))

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
            job-1 (sut/add client token draftset quads-1)
            graph-2 (URI. "http://test.graph.com/quad-graph2")
            quads-2 (map #(assoc % :c graph-2) triples)
            quads-2 (take how-many (drop how-many quads-2))
            job-2 (sut/add client token draftset quads-2)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client token job-2)))
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client token job-2)))
            quads* (sut/get client token draftset)]
        (t/is (= (set (concat quads-1 quads-2)) (set quads*)))))))

(t/deftest adding-quads-to-multiple-graphs-in-a-draftset
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
            job-1 (sut/add client token draftset graph-1 triples-1)
            graph-2 (URI. "http://test.graph.com/triple-graph2")
            triples-2 (take how-many (drop how-many triples))
            job-2 (sut/add client token draftset graph-2 triples-2)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client token job-1)))
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client token job-2)))
            triples-1* (sut/get client token draftset graph-1)
            triples-2* (sut/get client token draftset graph-2)]
        (t/is (= (set triples-1) (set triples-1*)))
        (t/is (= (set triples-2) (set triples-2*)))))))

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
      (let [async-job (sut/add client token draftset (take 5 quads))]
        (t/is (= "ok" (:type async-job)))
        (t/is (not (sut/job-complete? async-job)))
        (let [job-status (sut/resolve-job client token async-job)]
          (t/is (= :drafter-client.client/completed job-status)))))))


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
            job (sut/add client token draftset graph triples)
            _ (t/is (= :drafter-client.client/completed
                       (sut/resolve-job client token job)))
            repo (sut/->repo client token draftset)]
        (with-open [conn (gr-repo/->connection repo)]
          (let [res (gr-repo/query conn "ASK WHERE { ?s ?p ?o }")]
            (t/is (true? res))))))))


(t/deftest integrant-null-client
  (t/testing "missing a drafter uri key returns nil client"
    (t/is (nil? (ig/init-key :drafter-client/client {})))
    (t/is (nil? (ig/init-key :drafter-client/client {:drafter-uri ""})))
    (t/is (some? (ig/init-key :drafter-client/client
                              {:batch-size 10
                               :drafter-uri (env :drafter-endpoint)
                               :auth0-endpoint (env :auth0-domain)})))))
