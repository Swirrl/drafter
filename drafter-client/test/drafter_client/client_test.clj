(ns drafter-client.client-test
  (:require [drafter-client.client :as sut]
            [drafter-client.client.draftset :as draftset]
            [grafter.rdf.protocols :as gr-pr]
            [grafter.rdf.repository :as gr-repo]
            [clojure.test :as t]
            [integrant.core :as ig]
            [drafter-client.test-util.db :as db-util]
            [drafter-client.test-util.auth :as auth-util]
            [environ.core :refer [env]]
            [clojure.string :as str])
  (:import (java.net URI)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Setup ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *drafter-client*)
(def ^:dynamic *test-triples*)


(t/use-fixtures :each
  ;; Drop db after tests
  (fn [test-fn]

    (t/is (env :sparql-query-endpoint)
          "Set SPARQL_QUERY_ENDPOINT to run these tests.")
    (t/is (env :sparql-update-endpoint)
          "Set SPARQL_UPDATE_ENDPOINT to run these tests.")
    (let [stardog-query (env :sparql-query-endpoint)
          stardog-update (env :sparql-update-endpoint)
          stardog-repo (grafter.rdf.repository/sparql-repo stardog-query
                                                           stardog-update)]
      (when-not (Boolean/parseBoolean (env :disable-drafter-cleaning-protection))
        (db-util/assert-empty stardog-repo))
      (test-fn)
      (db-util/drop-all! stardog-repo)))
  ;; Setup drafter client
  (fn [test-fn]
    (t/is (env :drafter-endpoint) "Set DRAFTER_ENDPOINT to run these tests.")
    (let [batch-write-size 150000
          jws-key (env :drafter-jws-signing-key)
          drafter-endpoint (URI. (format "%s/v1" (env :drafter-endpoint)))
          drafter-client (sut/->drafter-client drafter-endpoint
                                               jws-key
                                               batch-write-size)]
      (binding [*drafter-client* drafter-client]
        (test-fn))))
  ;; Test triples
  (fn [test-fn]
    (let [file "./test/specific_mappingbased_properties_bg.nt"
          statements (grafter.rdf/statements file)]
      (binding [*test-triples* statements]
        (test-fn)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/deftest live-repo-tests
  (let [client *drafter-client*
        repo (sut/->repo client auth-util/system-user sut/live)]
    (t/testing "Live database is empty"
      (with-open [conn (gr-repo/->connection repo)]
        (let [result (gr-repo/query conn "ASK WHERE { ?s ?p ?o }")]
          (t/is (false? result)
                "I really expected the database to be empty"))))))


(t/deftest draftsets-tests
  (let [client *drafter-client*
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
  (let [client *drafter-client*
        triples *test-triples*
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
  (let [client *drafter-client*
        triples *test-triples*
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
  (let [client *drafter-client*
        triples *test-triples*
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
  (let [client *drafter-client*
        triples *test-triples*
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
  (let [client *drafter-client*
        graph (URI. "http://test.graph.com/3")
        triples *test-triples*
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
  (let [client *drafter-client*
        triples *test-triples*
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
    (t/is (some? (ig/init-key :drafter-client/client {:batch-size 10
                                                      :drafter-uri ""
                                                      :jws-key ""})))))
