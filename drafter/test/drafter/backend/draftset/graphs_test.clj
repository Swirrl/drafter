(ns drafter.backend.draftset.graphs-test
  (:require [drafter.backend.draftset.graphs :refer :all]
            [clojure.test :as t]
            [drafter.test-common :as tc]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.fixture-data :as fd]
            [grafter-2.rdf4j.repository :as repo]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.user-test :refer [test-editor]]
            [drafter.test-helpers.draft-management-helpers :as mgmth]
            [drafter.util :as util]
            [grafter-2.rdf4j.sparql :as sp]
            [grafter.url :as url])
  (:import [java.net URI]))

(t/deftest uri-matches?-test
  (t/are [matcher uri expected] (= expected (uri-matches? matcher uri))
    ;;regex
    #"http://example.com/graphs/.*" (URI. "http://example.com/graphs/test") true
    #"http://example.com/graphs/.*" (URI. "http://test.com/user") false

    ;;uri
    (URI. "http://example.com/test") (URI. "http://example.com/test") true
    (URI. "http://example.com/test") (URI. "http://test.com/uesr") false))

(t/deftest create-manager-test
  (let [repo (repo/memory-store)]
    (t/testing "Default protected graphs"
      (let [manager (create-manager repo)]
        (t/is (= true (protected-graph? manager mgmt/drafter-state-graph)) "Expected state graph to be protected")))

    (t/testing "Extra protected graphs"
      (let [protected-graphs [(URI. "http://protected")
                              (URI. "http://system-graph")]
            manager (create-manager repo protected-graphs)]
        (doseq [protected-graph protected-graphs]
          (t/is (protected-graph? manager protected-graph) "Expected graph to be protected"))))))

(t/deftest ensure-managed-user-graph-test
  (tc/with-system
    [system "repo.edn"]
    (let [repo (:drafter/backend system)]
      (t/testing "Valid user graph"
        (fd/drop-all! repo)
        (let [manager (create-manager repo)
              graph-uri (URI. "http://example.com/graphs/test")]
          (t/is (= false (mgmt/is-graph-managed? repo graph-uri)) "Graph exists before creation")
          (ensure-managed-user-graph manager graph-uri)
          (t/is (= true (mgmt/is-graph-managed? repo graph-uri)) "Expected managed graph to be created")))

      (t/testing "Protected graph"
        (fd/drop-all! repo)
        (let [protected-graph-uri (URI. "http://cant-touch-this")
              manager (create-manager repo #{protected-graph-uri})]
          (t/is (thrown? Exception (ensure-managed-user-graph manager protected-graph-uri)) "Expected creation to fail")
          (t/is (= false (mgmt/is-graph-managed? repo protected-graph-uri)) "Expected graph to not be created"))))))

(defn is-draft-of? [repo draftset-ref live-graph-uri draft-graph-uri]
  (let [draftset-uri (url/->java-uri draftset-ref)
        bindings {:draft draft-graph-uri
                  :live live-graph-uri
                  :ds draftset-uri}]
    (with-open [conn (repo/->connection repo)]
      (sp/query "drafter/backend/draftset/graphs_test/ask-is-graph-draft-for.sparql" bindings conn))))

(t/deftest create-user-graph-draft-test
  (tc/with-system
    [:drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          protected-graph (URI. "http://cant-touch-this")
          manager (create-manager repo #{protected-graph})
          draftset-id (dsops/create-draftset! repo test-editor)]
      (t/testing "When managed graph does not exist"
        (let [live-graph-uri (URI. "http://test-graph-1")
              draft-graph-uri (create-user-graph-draft manager draftset-id live-graph-uri)]
          (t/is (mgmt/is-graph-managed? repo live-graph-uri) "Expected managed graph to be created")
          (t/is (is-draft-of? repo draftset-id live-graph-uri draft-graph-uri) "Expected draft graph to be created")))

      (t/testing "When managed graph exists"
        (let [live-graph-uri (URI. "http://test-graph-2")]
          (ensure-managed-user-graph manager live-graph-uri)
          (let [draft-graph-uri (create-user-graph-draft manager draftset-id live-graph-uri)]
            (t/is (is-draft-of? repo draftset-id live-graph-uri draft-graph-uri) "Expected draft graph to be created"))))

      (t/testing "Protected graph"
        (t/is (thrown? Exception (create-user-graph-draft manager draftset-id protected-graph)) "Should not create draft of protected graph")
        (let [live->draft (dsops/get-draftset-graph-mapping repo draftset-id)]
          (t/is (= false (contains? live->draft protected-graph)) "Draft graph should not be created"))))))

(t/deftest delete-user-graph-not-in-live
  (tc/with-system
    [:drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          manager (create-manager repo)
          draftset-id (dsops/create-draftset! repo test-editor)
          graph-to-delete (URI. "http://missing")]
      (delete-user-graph manager draftset-id graph-to-delete)

      (t/is (= false (mgmt/is-graph-managed? repo graph-to-delete)) "Non-existent graph should not be created")
      (t/is (empty? (dsops/get-draftset-graph-mapping repo draftset-id)) "Draft graph should not be created"))))

(t/deftest delete-user-graph-not-in-draftset
  (tc/with-system
    [:drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          manager (create-manager repo)
          draftset-id (dsops/create-draftset! repo test-editor)
          live-graph (URI. "http://live")]
      (tc/make-graph-live! repo live-graph)
      (delete-user-graph manager draftset-id live-graph)

      (t/is (mgmt/is-graph-managed? repo live-graph) "Graph should still be managed")

      (let [graph-mapping (dsops/get-draftset-graph-mapping repo draftset-id)]
        (t/is (contains? graph-mapping live-graph) "Draft graph should exist for deleted graph")
        (t/is (mgmth/draft-exists? repo (get graph-mapping live-graph)) "Draft graph should exist for deleted graph")))))

(defn- get-modified-time [repo subject]
  (with-open [conn (repo/->connection repo)]
    (let [q (format "SELECT ?modified WHERE { <%s> <http://purl.org/dc/terms/modified> ?modified .}" subject)
          results (vec (repo/query conn q))]
      (-> results first :modified))))

(t/deftest delete-user-graph-in-draftset
  (tc/with-system
    [:drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    (let [clock (tc/incrementing-clock)
          repo (:drafter/backend system)
          manager (create-manager repo #{} clock)
          draftset-id (dsops/create-draftset! repo test-editor)
          live-graph (URI. "http://live")]

      (tc/make-graph-live! repo live-graph (tc/test-triples) clock)

      (let [draft-graph (tc/import-data-to-draft! repo live-graph (tc/test-triples (URI. "http://subject")) draftset-id)
            initially-modified-at (get-modified-time repo live-graph)]

        (t/is (mgmth/draft-exists? repo draft-graph) "Graph should still be managed")

        (delete-user-graph manager draftset-id live-graph)

        (let [subsequently-modified-at (get-modified-time repo draft-graph)]
          (t/is (.isBefore initially-modified-at subsequently-modified-at) "Draft graph modified time should be updated"))

        (t/is (mgmth/draft-exists? repo draft-graph) "Draft graph should still exist")
        (t/is (= true (mgmt/graph-empty? repo draft-graph)) "Draft graph should be empty")))))

(t/deftest delete-user-graph-protected
  (tc/with-system
    [:drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          protected-graph (URI. "http://cant-touch-this")
          manager (create-manager repo #{protected-graph})
          draftset-id (dsops/create-draftset! repo test-editor)]
      (tc/make-graph-live! repo protected-graph)
      (t/is (thrown? Exception (delete-user-graph manager draftset-id protected-graph)) "Should not delete protected graph")
      (let [live->draft (dsops/get-draftset-graph-mapping repo draftset-id)]
        (t/is (= false (contains? live->draft protected-graph)) "Should not create draft of protected graph")))))

(t/deftest ensure-protected-graph-draft-empty-test
  (tc/with-system
    [:drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          protected-graph (URI. "http://protected")
          manager (create-manager repo #{protected-graph})
          draftset-id (dsops/create-draftset! repo test-editor)]
      (let [draft-graph-uri (ensure-protected-graph-draft manager draftset-id protected-graph)]
        (t/is (mgmt/is-graph-managed? repo protected-graph) "Expected managed graph to be created")
        (t/is (is-draft-of? repo draftset-id protected-graph draft-graph-uri) "Expected draft graph to be created")))))

(t/deftest ensure-protected-graph-draft-graph-exists-test
  ;; ensuring an existing draft graph exists should return the existing draft graph
  (tc/with-system
    [:drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          protected-graph (URI. "http://protected")
          manager (create-manager repo #{protected-graph})
          draftset-id (dsops/create-draftset! repo test-editor)]
      (let [draft-graph-uri-1 (ensure-protected-graph-draft manager draftset-id protected-graph)
            draft-graph-uri-2 (ensure-protected-graph-draft manager draftset-id protected-graph)]
        (t/is (= draft-graph-uri-1 draft-graph-uri-2) "Expected existing draft to be returned")))))

(t/deftest ensure-protected-graph-draft-invalid-protected-graph-test
  (tc/with-system
    [:drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    (let [repo (:drafter/backend system)
          graph (URI. "http://not-protected")
          manager (create-manager repo)
          draftset-id (dsops/create-draftset! repo test-editor)]
      (t/is (thrown? Exception (ensure-protected-graph-draft manager draftset-id graph)) "Graph should not be protected"))))