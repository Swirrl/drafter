(ns drafter.feature.modified-times-test
  (:require [clojure.test :as t :refer :all]
            [drafter.feature.modified-times :as sut :refer [query-modification-times]]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-publisher]]
            [grafter-2.rdf4j.repository :as repo]
            [drafter.draftset :as draftset]
            [grafter.url :as url]
            [drafter.backend.draftset :as backend-draftset]
            [drafter.backend.draftset.draft-management :as mgmt]
            [drafter.rdf.drafter-ontology :refer [drafter:graph-modified modified-times-graph-uri]]
            [grafter.vocabularies.dcterms :refer [dcterms:issued dcterms:modified]]
            [clojure.java.io :as io]
            [grafter-2.rdf4j.io :as rio]
            [drafter.model :as model]
            [drafter.rdf.sesame :as ses]
            [drafter.fixture-data :as fd]
            [drafter.backend.draftset.graphs :as graphs]
            [drafter.rdf.sparql :as sparql]
            [drafter.rdf.jena :as jena]
            [drafter.test-helpers.draft-management-helpers :as mgmt-helpers]
            [drafter.util :as util :refer [make-quads]]
            [drafter.generators :as gens])
  (:import [java.net URI]
           [java.time ZoneOffset OffsetDateTime]))

;; Modifications should not create state graph modifications for draft graphs
;; Modifications *should* update dcterms:modified for draftsets
;; Publish process should not create dcterms:issued timestamps in the state graph

(defn- exec-query [resource bindings conn]
  (let [q (slurp (io/resource resource))
        pq (repo/prepare-query conn q)]
    (doseq [[key value] bindings]
      (.setBinding pq (name key) (rio/->backend-type value)))
    (repo/evaluate pq)))

(defn- query-state-graph-timestamp-for [repo subject timestamp-pred]
  (let [q (format "SELECT ?timestamp WHERE { GRAPH <%s> { <%s> <%s> ?timestamp } }"
                  mgmt/drafter-state-graph
                  subject
                  timestamp-pred)]
    (with-open [conn (repo/->connection repo)]
      (let [bindings (vec (repo/query conn q))]
        (-> bindings first :timestamp)))))

(defn- get-state-graph-timestamp-for [drafter subject timestamp-pred]
  (query-state-graph-timestamp-for (model/get-raw-repo drafter) subject timestamp-pred))

(defn- get-state-graph-modified-for
  "Returns the state graph dcterms:modified timestamp for subject"
  [drafter subject]
  (get-state-graph-timestamp-for drafter subject dcterms:modified))

(defn- draft-graph-exists-for? [drafter draft live-graph-uri]
  (mgmt-helpers/draft-graph-exists-for? (model/get-raw-repo drafter) draft live-graph-uri))

(defn- query-draft-modifications-graph-exists? [repo draft]
  (mgmt-helpers/draft-graph-exists-for? repo draft modified-times-graph-uri))

(defn- draft-modifications-graph-exists? [drafter draft]
  (draft-graph-exists-for? drafter draft modified-times-graph-uri))

(defn- live-modifications-graph-exists? [drafter]
  (let [repo (model/get-raw-repo drafter)]
    (mgmt/is-graph-live? repo modified-times-graph-uri)))

(defn- get-draft-modification-times [drafter draft]
  (query-modification-times (model/get-draftset-query-endpoint drafter nil draft)))

(defn- get-live-modification-times [drafter]
  (query-modification-times (model/get-live-query-endpoint drafter)))

(defn- get-test-repo [system]
  (repo/sparql-repo (:drafter.common.config/sparql-query-endpoint system)
                    (:drafter.common.config/sparql-update-endpoint system)))

(defn- append-triples [drafter user draftset-ref graph triples]
  (let [source (ses/->GraphTripleStatementSource (ses/->CollectionStatementSource triples) graph)]
    (model/append-data drafter user draftset-ref source)))

(defn- publish-quads [drafter quads]
  (let [draft (model/create-draftset drafter test-publisher)]
    (model/append-data drafter test-publisher draft (ses/->CollectionStatementSource quads))
    (model/publish-draftset drafter test-publisher draft)))

(defn- delete-triples [drafter user draft graph triples]
  (let [quads (make-quads graph triples)
        source (ses/->CollectionStatementSource quads)]
    (model/delete-data drafter user draft source)))

(defn- query-draftset-modified [repo draft]
  (query-state-graph-timestamp-for repo (url/->java-uri draft) dcterms:modified))

(defn- get-draftset-modified [drafter draft]
  (query-draftset-modified (model/get-raw-repo drafter) draft))

;;append tests

(t/deftest append-new-graph-from-empty
  (testing "Appending to a new graph with no live modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2020 12 1 0 0 0 0 ZoneOffset/UTC)
            clock (tc/manual-clock t1)]

        ;; 1. empty database
        ;; 2. append to new graph within draft
        ;;   => graph should have draft modification time
        ;;   => draftset should be modified
        ;; 3. publish
        ;;   => modifications graph should be published to live

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [draft (model/create-draftset drafter test-publisher {})
                graph (URI. "http://example.com/graph/1")]
            (append-triples drafter test-publisher draft graph (gens/generate-triples))

            (t/is (= true (draft-modifications-graph-exists? drafter draft)))
            (t/is (= {graph t1 modified-times-graph-uri t1} (get-draft-modification-times drafter draft)))

            (t/is (= t1 (get-draftset-modified drafter draft)))

            (model/publish-draftset drafter test-publisher draft)

            (t/is (live-modifications-graph-exists? drafter))
            (t/is (= {graph t1 modified-times-graph-uri t1} (get-live-modification-times drafter)))))))))

(t/deftest append-to-existing-draft-graph-test
  (t/testing "Appending to a draft graph in an existing draft"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            clock (tc/manual-clock t1)
            graph (URI. "http://example.com/graph/1")]

        ;; 1. empty database
        ;; 2. append to new graph within a draft
        ;; 3. append more triples to graph
        ;;  => graph modified time should be updated
        ;;  => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [draft (model/create-draftset drafter test-publisher)]
            (append-triples drafter test-publisher draft graph (gens/generate-triples))

            ;; append more triples to the draft graph
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft graph (gens/generate-triples))

            ;; draft graph and modifications graph timestamps should be updated
            (t/is (= {graph t2 modified-times-graph-uri t2} (get-draft-modification-times drafter draft)))

            ;; draftset modified time should be updated
            (t/is (= t2 (get-draftset-modified drafter draft)))))))))

(t/deftest append-to-live-graph-test
  (t/testing "Appending within a new draft to a live graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusDays t2 2)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. graph-1 and graph-2 exist in live
        ;; 2. append to graph-1 within a draft
        ;;   => draft modifications graph should be created containing graph-1 and modifications timestamps
        ;;   => graph-2 should not exist in the draft modifications graph
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [initial-quads (concat (make-quads graph-1 (gens/generate-triples))
                                      (make-quads graph-2 (gens/generate-triples)))]
            (publish-quads drafter initial-quads)

            ;; update time and append more triples to graph-1
            (tc/set-now clock t2)
            (let [draft (model/create-draftset drafter test-publisher)]
              (append-triples drafter test-publisher draft graph-1 (gens/generate-triples))

              (t/is (= {graph-1                  t2
                        modified-times-graph-uri t2} (get-draft-modification-times drafter draft)))

              (t/is (= t2 (get-draftset-modified drafter draft)))

              ;; publish draft
              (tc/set-now clock t3)
              (model/publish-draftset drafter test-publisher draft)

              ;; modifications graph should be in live
              (t/is (= {graph-1 t2
                        graph-2 t1
                        modified-times-graph-uri t2} (get-live-modification-times drafter))))))))))

;; revert graph

(t/deftest revert-only-changed-graph-test
  (t/testing "Revert change to only graph changed within a draftset"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 41)
            t4 (.plusMinutes t3 5)
            clock (tc/manual-clock t1)
            graph (URI. "http://example.com/graph/1")]

        ;; 1. graph exists in live
        ;; 2. modify graph within a draft
        ;; 3. revert graph changes in draft
        ;;   => no draft changes so draft modifications graph should not exist
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph (gens/generate-triples)))

          (tc/set-now clock t2)
          (let [initial-modification-times (get-live-modification-times drafter)
                draft (model/create-draftset drafter test-publisher)]
            ;; make changes to graph within draft then revert
            (append-triples drafter test-publisher draft graph (gens/generate-triples))

            (tc/set-now clock t3)
            (model/revert-graph-changes drafter test-publisher draft graph)

            ;; no longer any draft changes so modifications graph should be removed
            (t/is (= false (draft-modifications-graph-exists? drafter draft)))

            (t/is (= t3 (get-draftset-modified drafter draft)))

            (tc/set-now clock t4)
            (model/publish-draftset drafter test-publisher draft)

            ;; graph and modifications graph timestamps should not be updated
            (t/is (= initial-modification-times (get-live-modification-times drafter)))))))))

(t/deftest revert-graph-in-draft-containing-other-graph-changes-test
  (t/testing "Revert change to graph within a draft that contains other graph changes"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            t4 (.plusMinutes t3 48)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. graph-1 and graph-2 exist in live
        ;; 2. make changes to both graphs within a draft
        ;; 3. revert changes to graph-2
        ;;   => modification time of graph-2 should be removed from the draft modifications graph
        ;;   => modification time of graph-1 should still be updated within the draft
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [initial-quads (concat (make-quads graph-1 (gens/generate-triples))
                                      (make-quads graph-2 (gens/generate-triples)))]
            (publish-quads drafter initial-quads))

          (tc/set-now clock t2)
          (let [draft (model/create-draftset drafter test-publisher)
                new-quads (concat (make-quads graph-1 (gens/generate-triples))
                                  (make-quads graph-2 (gens/generate-triples)))]
            ;; make changes to both graphs then revert changes to graph-2
            (model/append-data drafter test-publisher draft (ses/->CollectionStatementSource new-quads))

            (tc/set-now clock t3)
            (model/revert-graph-changes drafter test-publisher draft graph-2)

            ;; graph-2 should be removed from the draft modifications graph
            ;; modified times for graph-1 and the modifications graph should be updated
            (t/is (= {graph-1                  t2
                      modified-times-graph-uri t3} (get-draft-modification-times drafter draft)))

            (t/is (= t3 (get-draftset-modified drafter draft)))

            (tc/set-now clock t4)
            (model/publish-draftset drafter test-publisher draft)

            ;; live modified graph should be updated
            (t/is (= {graph-1                  t2
                      graph-2                  t1
                      modified-times-graph-uri t3}
                     (get-live-modification-times drafter)))))))))

(t/deftest revert-graph-delete-test
  (t/testing "Reverting a graph deletion which is the only change should remove the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            clock (tc/manual-clock t1)
            graph (URI. "http://example.com/graph/1")]

        ;; 1. graph exists in live
        ;; 2. graph is deleted within a draft
        ;; 3. graph delete is reverted within the draft
        ;; 4.  => no visible changes so draft modifications graph should be removed
        ;;     => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph (gens/generate-triples)))

          (tc/set-now clock t2)
          (let [initial-modification-times (get-live-modification-times drafter)
                draft (model/create-draftset drafter test-publisher)]
            (model/delete-graph drafter test-publisher draft graph)
            (model/revert-graph-changes drafter test-publisher draft graph)

            ;; modifications graph should be removed
            (t/is (= false (draft-modifications-graph-exists? drafter draft)))
            (t/is (= {} (get-draft-modification-times drafter draft)))

            (t/is (= t2 (get-draftset-modified drafter draft)))

            (tc/set-now clock t3)
            (model/publish-draftset drafter test-publisher draft)

            ;; no changes should be published to the modification times
            (t/is (= initial-modification-times (get-live-modification-times drafter)))))))))

(t/deftest revert-graph-delete-in-draft-with-other-changes-test
  (t/testing "Reverting a graph deletion from a draft with other changes should reset the graph modified time"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            t4 (.plusMinutes t3 38)
            t5 (.plusMinutes t4 13)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")
            initial-quads (concat (make-quads graph-1 (gens/generate-triples))
                                  (make-quads graph-2 (gens/generate-triples)))]

        ;; 1. graph-1 and graph-2 exist in live
        ;; 2. graph-1 is modified within a draft
        ;; 3. graph-2 is deleted within the draft
        ;; 4. deletion of graph-2 is reverted within the draft
        ;;   => modification for graph-2 should be removed
        ;;   => graph-1 should still be modified
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter initial-quads)

          (tc/set-now clock t2)
          (let [draft (model/create-draftset drafter test-publisher)]
            ;; append to graph-1 and delete graph-2
            (append-triples drafter test-publisher draft graph-1 (gens/generate-triples))

            (tc/set-now clock t3)
            (model/delete-graph drafter test-publisher draft graph-2)

            ;; revert deletion of graph-2
            (tc/set-now clock t4)
            (model/revert-graph-changes drafter test-publisher draft graph-2)

            ;; graph-1 still has changes so modifications graph should exist
            ;; draft modification for graph-2 should be removed
            ;; draft modification graph modified time (and draftset) should be updated
            (t/is (= true (draft-modifications-graph-exists? drafter draft)))
            (t/is (= {graph-1                  t2
                      modified-times-graph-uri t4} (get-draft-modification-times drafter draft)))

            (t/is (= t4 (get-draftset-modified drafter draft)))

            ;; publish
            (tc/set-now clock t5)
            (model/publish-draftset drafter test-publisher draft)

            ;; modification changes should be visible in live
            (t/is (= {graph-1                  t2
                      graph-2                  t1
                      modified-times-graph-uri t4} (get-live-modification-times drafter)))))))))

;; delete data
(t/deftest delete-data-test
  (t/testing "Deleting some graph data within a new draft should create draft modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")
            initial-graph-1-triples (gens/generate-triples 10)]

        ;; 1. graph-1 and graph-2 exist in live
        ;; 2. delete some triples from graph-1
        ;;   => modification time of graph-1 should be updated in the draft
        ;;   => modification time of draft should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [initial-quads (concat (make-quads graph-1 initial-graph-1-triples)
                                      (make-quads graph-2 (gens/generate-triples)))]
            (publish-quads drafter initial-quads))

          (let [draft (model/create-draftset drafter test-publisher)
                to-delete (take 4 initial-graph-1-triples)]
            (tc/set-now clock t2)
            (delete-triples drafter test-publisher draft graph-1 to-delete)

            ;; graph-1 and modified graph modified times should be updated
            (t/is (= true (draft-modifications-graph-exists? drafter draft)))
            (t/is (= {graph-1                  t2
                      modified-times-graph-uri t2} (get-draft-modification-times drafter draft)))

            (t/is (= t2 (get-draftset-modified drafter draft)))

            (tc/set-now clock t3)
            (model/publish-draftset drafter test-publisher draft)

            ;; live modifications graph should be updated
            (t/is (= {graph-1 t2
                      graph-2 t1
                      modified-times-graph-uri t2} (get-live-modification-times drafter)))))))))

;; deleting all data for graph which exists only within a draft
(t/deftest delete-all-draft-graph-data-in-draft-with-no-other-changes-test
  (t/testing "Deleting all graph data that exists only within a draft with no other changes should remove the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 58)
            clock (tc/manual-clock t1)
            graph (URI. "http://example.com/graph/1")
            draft-triples (gens/generate-triples)]

        ;; 1. create a new graph within a draft
        ;; 2. delete all graph data
        ;;   => no visible draft changes so modifications graph should not exist
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [draft (model/create-draftset drafter test-publisher)]
            (append-triples drafter test-publisher draft graph draft-triples)
            (tc/set-now clock t2)
            (delete-triples drafter test-publisher draft graph draft-triples)

            ;; draft now contains no changes so modifications graph should not exist
            (t/is (= false (draft-modifications-graph-exists? drafter draft)))
            (t/is (= {} (get-draft-modification-times drafter draft)))

            (t/is (= t2 (get-draftset-modified drafter draft)))

            (tc/set-now clock t3)
            (model/publish-draftset drafter test-publisher draft)

            ;; modifications graoh should not be created
            (t/is (= false (live-modifications-graph-exists? drafter)))))))))

(t/deftest delete-all-draft-graph-data-in-draft-with-other-changes-test
  (t/testing "Deleting all graph data that exists only within a graph with other changes should remove it from the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 15)
            t4 (.plusMinutes t3 39)
            t5 (.plusHours t4 19)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")
            initial-graph-1-triples (gens/generate-triples 10)]

        ;; 1. graph-1 exists in live
        ;; 2. create graph-2 within a draft
        ;; 3. modify graph-1
        ;; 4. delete graph-2 data within the draft
        ;;   => graph-1 modified time should be updated
        ;;   => graph-2 should not be in the modifications graph
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph-1 initial-graph-1-triples))

          (tc/set-now clock t2)
          (let [draft (model/create-draftset drafter test-publisher)
                graph-2-triples (gens/generate-triples)]
            ;; create graph-2 within the draft
            (append-triples drafter test-publisher draft graph-2 graph-2-triples)

            ;; modify graph-1 within the draft
            (tc/set-now clock t3)
            (delete-triples drafter test-publisher draft graph-1 (take 3 initial-graph-1-triples))

            ;; delete graph-2 within the draft
            (tc/set-now clock t4)
            (delete-triples drafter test-publisher draft graph-2 graph-2-triples)

            ;; draft modifications graph should exist
            ;; graph-1 should be modified
            ;; modifications graph should have modification time of delete of graph-2
            ;; no entry for graph-2 should exist
            (let [expected-modified-times {graph-1 t3
                                           modified-times-graph-uri t4}]
              (t/is (= true (draft-modifications-graph-exists? drafter draft)))
              (t/is (= expected-modified-times (get-draft-modification-times drafter draft)))

              (t/is (= t4 (get-draftset-modified drafter draft)))

              ;; publish
              (tc/set-now clock t5)
              (model/publish-draftset drafter test-publisher draft)

              ;; live modifications graph should be updated
              (t/is (= expected-modified-times (get-live-modification-times drafter))))))))))

(t/deftest delete-all-live-graph-data-in-draftset-with-no-other-changes-test
  (t/testing "Deleting all data from a live graph within a draft with no other changes should update the graph modified time"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")
            graph-1-triples (gens/generate-triples)]

        ;; 1. graph-1 and graph-2 exist in live
        ;; 2. all graph-1 data is deleted within a draft
        ;;   => modified time of graph-1 should be updated
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [initial-quads (concat (make-quads graph-1 graph-1-triples)
                                      (make-quads graph-2 (gens/generate-triples)))]
            (publish-quads drafter initial-quads))

          (tc/set-now clock t2)
          (let [draft (model/create-draftset drafter test-publisher)]
            (delete-triples drafter test-publisher draft graph-1 graph-1-triples)

            ;; modification time of graph-1 and the modifications graph should be updated
            (t/is (= true (draft-modifications-graph-exists? drafter draft)))
            (t/is (= {graph-1                  t2
                      modified-times-graph-uri t2} (get-draft-modification-times drafter draft)))

            (t/is (= t2 (get-draftset-modified drafter draft)))

            (tc/set-now clock t3)
            (model/publish-draftset drafter test-publisher draft)

            ;; modifications graph changes should be published
            ;; graph-2 should be unaffected in live
            (t/is (= {graph-1                  t2
                      graph-2                  t1
                      modified-times-graph-uri t2} (get-live-modification-times drafter)))))))))

(t/deftest delete-all-graph-data-in-draftset-with-other-changes-test
  (t/testing "Deleting all graph data within a draft with other changes should update the graph modified time"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            t4 (.plusMinutes t3 78)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")
            graph-1-triples (gens/generate-triples)]

        ;; 1. graph-1 and graph-2 exist in live
        ;; 2. graph-2 is modified within a draft
        ;; 3. graph-1 is deleted within the draft
        ;;   => modified time of graph-1 and graph-2 should be updated
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [initial-quads (concat (make-quads graph-1 graph-1-triples)
                                      (make-quads graph-2 (gens/generate-triples)))]
            (publish-quads drafter initial-quads))

          (tc/set-now clock t2)
          (let [draft (model/create-draftset drafter test-publisher)]
            (append-triples drafter test-publisher draft graph-2 (gens/generate-triples))

            (tc/set-now clock t3)
            (delete-triples drafter test-publisher draft graph-1 graph-1-triples)

            (let [expected-modification-times {graph-1 t3
                                               graph-2 t2
                                               modified-times-graph-uri t3}]
              ;; draft contains changes so draft modifications graph should exist
              (t/is (= true (draft-modifications-graph-exists? drafter draft)))
              (t/is (= expected-modification-times (get-draft-modification-times drafter draft)))

              (t/is (= t3 (get-draftset-modified drafter draft)))

              ;; publish
              (tc/set-now clock t4)
              (model/publish-draftset drafter test-publisher draft)

              ;; modification graph changes should be in live
              (t/is (= expected-modification-times (get-live-modification-times drafter))))))))))

(t/deftest delete-non-existent-graph-data-test
  (t/testing "Deleting data for a non-existent graph should not clone the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            clock (tc/manual-clock t1)
            graph (URI. "http://example.com/graph/1")
            missing-graph (URI. "http://missing")]

        ;; 1. graph exists in live
        ;; 2. delete data from a non-existent graph within a draft
        ;;   => no visible draft changes so draft modifications should not exist
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph (gens/generate-triples)))

          (tc/set-now clock t2)
          (let [initial-modification-times (get-live-modification-times drafter)
                draft (model/create-draftset drafter test-publisher)]
            (delete-triples drafter test-publisher draft missing-graph (gens/generate-triples))

            ;; draft modifications graph should not exist
            (t/is (= false (draft-modifications-graph-exists? drafter draft)))
            (t/is (= t2 (get-draftset-modified drafter draft)))

            (tc/set-now clock t3)
            (model/publish-draftset drafter test-publisher draft)

            ;; live modifications graph should not be changed
            (t/is (= initial-modification-times (get-live-modification-times drafter)))))))))

;; delete graph

(t/deftest delete-graph-in-new-draftset-test
  (t/testing "Deleting a graph within a draftset should update the modified time"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. graph-1 and graph-2 exist in live
        ;; 2. graph-1 is deleted inside a new draftset
        ;;   => modifications graph should be created
        ;;   => graph-1 and modifications graph should be modified
        ;;   => graph-2 should not be included
        ;;   => draftset should be modified

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [initial-quads (concat (make-quads graph-1 (gens/generate-triples))
                                      (make-quads graph-2 (gens/generate-triples)))]
            (publish-quads drafter initial-quads))

          (tc/set-now clock t2)
          (let [draft (model/create-draftset drafter test-publisher)]
            (model/delete-graph drafter test-publisher draft graph-1)

            ;; draft modifications graph should be created
            ;; graph-1 and modifications graph should be updated
            (t/is (draft-modifications-graph-exists? drafter draft))
            (t/is (= {graph-1                  t2
                      modified-times-graph-uri t2} (get-draft-modification-times drafter draft)))

            (t/is (= t2 (get-draftset-modified drafter draft)))

            (tc/set-now clock t3)
            (model/publish-draftset drafter test-publisher draft)

            ;; modification graph changes should be published
            (t/is (= {graph-1 t2
                      graph-2 t1
                      modified-times-graph-uri t2} (get-live-modification-times drafter)))))))))

(t/deftest delete-graph-in-draftset-with-existing-modifications-test
  (t/testing "Deleting a graph within an existing draftset should update the modified time"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            t4 (.plusMinutes t3 24)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. graph-1 and graph-2 exist in live
        ;; 2. modify graph-1 within the draft
        ;; 3. delete graph-2 within the draft
        ;;   => both graph-1 and graph-2 should be modified
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [initial-quads (concat (make-quads graph-1 (gens/generate-triples))
                                      (make-quads graph-2 (gens/generate-triples)))]
            (publish-quads drafter initial-quads))

          (tc/set-now clock t2)
          (let [draft (model/create-draftset drafter test-publisher)]
            (append-triples drafter test-publisher draft graph-1 (gens/generate-triples))

            (tc/set-now clock t3)
            (model/delete-graph drafter test-publisher draft graph-2)

            ;; draft modifications graph should exist
            (let [expected-modified-times {graph-1 t2
                                           graph-2 t3
                                           modified-times-graph-uri t3}]
              (t/is (draft-modifications-graph-exists? drafter draft))
              (t/is (= expected-modified-times (get-draft-modification-times drafter draft)))

              (t/is (= t3 (get-draftset-modified drafter draft)))

              (tc/set-now clock t4)
              (model/publish-draftset drafter test-publisher draft)

              ;; modification graph changes should be published
              (t/is (= expected-modified-times (get-live-modification-times drafter))))))))))

(t/deftest delete-non-existent-graph-test
  (t/testing "Silently deleting a non-existent graph should not result in the modifications graph being cloned"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            clock (tc/manual-clock t1)
            graph (URI. "http://example.com/graph/1")]

        ;; 1. graph exists in live
        ;; 2. non-existent graph is deleted (silently) within a draft
        ;;   => no visible changes in draft so draft modifications graph should not exist
        ;;   => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph (gens/generate-triples)))

          (tc/set-now clock t2)
          (let [initial-modification-times (get-live-modification-times drafter)
                draft (model/create-draftset drafter test-publisher)
                missing-graph (URI. "http://missing")]
            (model/delete-graph drafter test-publisher draft missing-graph {:silent true})

            (t/is (= false (draft-graph-exists-for? drafter draft modified-times-graph-uri)))
            (t/is (= {} (get-draft-modification-times drafter draft)))

            (t/is (= t2 (get-draftset-modified drafter draft)))

            (tc/set-now clock t3)
            (model/publish-draftset drafter test-publisher draft)

            (t/is (= initial-modification-times (get-live-modification-times drafter)))))))))

;; publish
(t/deftest modification-times-monotonic-on-publish-test
  (t/testing "Publishing an older modification should still increase the modification time"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            t4 (.plusMinutes t3 145)
            clock (tc/manual-clock t1)
            graph (URI. "http://example.com/graph/1")]

        ;; 1. graph exists in live
        ;; 2. graph is modified in draft-1
        ;; 3. graph is modified (later) in draft-2
        ;; 4. draft-2 is published
        ;; 5. draft-1 (containing the earlier modification) is published
        ;;   => modified time of graph should be the publish time NOT the one in draft-1

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [initial-quads (make-quads graph (gens/generate-triples))]
            (publish-quads drafter initial-quads))

          ;; create two separate drafts which make modifications to the live graph
          (let [draft-1 (model/create-draftset drafter test-publisher)
                draft-2 (model/create-draftset drafter test-publisher)]
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft-1 graph (gens/generate-triples))

            (tc/set-now clock t3)
            (append-triples drafter test-publisher draft-2 graph (gens/generate-triples))

            ;; publish the most recent draft changes followed by the first
            (tc/set-now clock t4)
            (model/publish-draftset drafter test-publisher draft-2)
            (model/publish-draftset drafter test-publisher draft-1)

            ;; live modification time should be publish time instead of the modification time
            ;; within the draft
            (t/is (= {graph t4 modified-times-graph-uri t4} (get-live-modification-times drafter)))))))))

;; SPARQL Update queries
;; DROP GRAPH
(t/deftest drop-live-graph-test
  (t/testing "Dropping a live graph via a SPARQL UPDATE should update the modification time"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. graph-1 and graph-2 exist in live
        ;; 2. graph-1 dropped within a graph via the SPARQL Update endpoint
        ;;    => graph-1 and modifications graphs should be in draft modifications graph
        ;;    => graph-2 should not be included
        ;;    => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [initial-quads (concat (make-quads graph-1 (gens/generate-triples))
                                      (make-quads graph-2 (gens/generate-triples)))]
            (publish-quads drafter initial-quads))

          (let [draft (model/create-draftset drafter test-publisher)
                drop-query (format "DROP GRAPH <%s>" graph-1)]
            (tc/set-now clock t2)
            (model/submit-update-request drafter test-publisher draft drop-query)

            (t/is (draft-modifications-graph-exists? drafter draft))
            (t/is (= {graph-1 t2
                      modified-times-graph-uri t2} (get-draft-modification-times drafter draft)))

            (t/is (= t2 (get-draftset-modified drafter draft)))

            (tc/set-now clock t3)
            (model/publish-draftset drafter test-publisher draft)

            (t/is (= {graph-1 t2
                      graph-2 t1
                      modified-times-graph-uri t2} (get-live-modification-times drafter)))))))))

(t/deftest drop-draft-only-graph-in-draft-with-other-changes-test
  (t/testing "Dropping a graph that only exists in a draft via a SPARQL UPDATE should remove it from the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            t4 (.plusMinutes t3 145)
            t5 (.plusMinutes t4 36)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. graph-1 exists in live
        ;; 2. graph-2 is created within a draft
        ;; 3. graph-1 is modified within the draft
        ;; 4. graph-2 is DROPped within the draft
        ;;    => draft should contain modifications graph containing itself and graph-1
        ;;    => graph-2 should not be visible in the modifications graph
        ;;    => draftset modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [initial-quads (make-quads graph-1 (gens/generate-triples))]
            (publish-quads drafter initial-quads))

          (let [draft (model/create-draftset drafter test-publisher)
                drop-query (format "DROP GRAPH <%s>" graph-2)]
            ;; create graph-2 in new draft
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft graph-2 (gens/generate-triples))

            ;; modify graph-1 in draft
            (tc/set-now clock t3)
            (append-triples drafter test-publisher draft graph-1 (gens/generate-triples))

            ;; drop graph-2 in draft
            ;; graph-2 is only visible in the draft so should be removed from the modifications graph
            (tc/set-now clock t4)
            (model/submit-update-request drafter test-publisher draft drop-query)

            (t/is (= {graph-1 t3
                      modified-times-graph-uri t4} (get-draft-modification-times drafter draft)))

            (t/is (= t4 (get-draftset-modified drafter draft)))

            (tc/set-now clock t5)
            (model/publish-draftset drafter test-publisher draft)

            (t/is (= {graph-1 t3
                      modified-times-graph-uri t4} (get-live-modification-times drafter)))))))))

(t/deftest drop-last-draft-only-graph-test
  (t/testing "Dropping a graph which exists only in a draft with no other changes should remove the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            t4 (.plusMinutes t3 145)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. graph-1 exists in live
        ;; 2. graph-2 is created within a draft
        ;; 3. graph-2 is DROPped within the draft
        ;;    => no draft changes so no modifications graph should not exist
        ;;    => draftset modified time should be updated
        ;; 4. publish draft
        ;;    => live modifications graph should be unchanged

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph-1 (gens/generate-triples)))

          (let [initial-modifications (get-live-modification-times drafter)
                draft (model/create-draftset drafter test-publisher)
                drop-query (format "DROP GRAPH <%s>" graph-2)]
            ;; create graph in new draft
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft graph-2 (gens/generate-triples))

            ;; drop graph in draft
            ;; no visible draft changes so should be removed from the modifications graph
            (tc/set-now clock t3)
            (model/submit-update-request drafter test-publisher draft drop-query)

            (t/is (= false (draft-modifications-graph-exists? drafter draft)))
            (t/is (= {} (get-draft-modification-times drafter draft)))

            (t/is (= t3 (get-draftset-modified drafter draft)))

            (tc/set-now clock t4)
            (model/publish-draftset drafter test-publisher draft)

            (t/is (= initial-modifications (get-live-modification-times drafter)))))))))

(t/deftest drop-silent-non-existent-live-graph-test
  (t/testing "Silently Dropping a non-existent live graph should not update the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusHours t2 18)
            t4 (.plusMinutes t3 145)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. graph-1 exists in live
        ;; 2. graph-1 is modified within the draft
        ;; 3. non-existent graph graph-2 is DROPped within a draft
        ;;    => draft modifications graph should not be modified
        ;;    => draftset modified time should NOT be updated
        ;; 4. publish draft
        ;;    => draft modifications should be published to live

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph-1 (gens/generate-triples)))

          (let [draft (model/create-draftset drafter test-publisher)
                drop-query (format "DROP SILENT GRAPH <%s>" graph-2)]
            ;; modify graph-1 in the draft
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft graph-1 (gens/generate-triples))

            (let [draft-modifications (get-draft-modification-times drafter draft)]
              ;; drop non-existent live graph
              (tc/set-now clock t3)
              (model/submit-update-request drafter test-publisher draft drop-query)

              ;; draft modifications should be unchanged
              (t/is (= draft-modifications (get-draft-modification-times drafter draft)))

              (t/is (= t2 (get-draftset-modified drafter draft)))

              ;; publish draftset
              (tc/set-now clock t4)
              (model/publish-draftset drafter test-publisher draft)

              (t/is (= draft-modifications (get-live-modification-times drafter))))))))))

;; INSERT DATA

(t/deftest insert-data-into-new-graphs-test
  (t/testing "Adding to a new graph with INSERT DATA should create the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. database is empty
        ;; 2. INSERT DATA into graph-1 and graph-2 within a new draft
        ;;    => draft modifications graph should contain entries for itself and graph-1 and graph-2
        ;;    => draftset modification time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [draft (model/create-draftset drafter test-publisher)
                to-insert (concat (make-quads graph-1 (gens/generate-triples 4))
                                  (make-quads graph-2 (gens/generate-triples 6)))
                insert-query (util/quads->insert-data-query to-insert)
                expected-modifications {graph-1 t1
                                        graph-2 t1
                                        modified-times-graph-uri t1}]

            (model/submit-update-request drafter test-publisher draft insert-query)

            (t/is (draft-modifications-graph-exists? drafter draft))
            (t/is (= expected-modifications (get-draft-modification-times drafter draft)))

            (t/is (= t1 (get-draftset-modified drafter draft)))

            ;; publish draft
            (tc/set-now clock t2)
            (model/publish-draftset drafter test-publisher draft)

            (t/is (= expected-modifications (get-live-modification-times drafter)))))))))

(t/deftest insert-data-into-new-graph-in-existing-draft-test
  (t/testing "Adding data to a new graph within an existing draft with INSERT DATA should update the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            t4 (.plusMinutes t3 9)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. live contains graph-1
        ;; 2. draft contains modifications to graph-1
        ;; 3. INSERT DATA into new graph-2 within the draft
        ;;    => graph-2 should exist in the modifications graph
        ;;    => modified time of modifications graph should be updated
        ;;    => modified time of draftset should be updated
        ;; 4. publish draft
        ;; 5. modifications should be published to live

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph-1 (gens/generate-triples)))

          (let [draft (model/create-draftset drafter test-publisher)
                to-insert (make-quads graph-2 (gens/generate-triples))
                insert-query (util/quads->insert-data-query to-insert)]

            ;; update graph-1 within the new draft
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft graph-1 (gens/generate-triples))

            ;; INSERT DATA into new graph-2
            (tc/set-now clock t3)
            (model/submit-update-request drafter test-publisher draft insert-query)

            (let [expected-modifications {graph-1                  t2
                                          graph-2                  t3
                                          modified-times-graph-uri t3}]

              (t/is (= expected-modifications (get-draft-modification-times drafter draft)))

              (t/is (= t3 (get-draftset-modified drafter draft)))

              (tc/set-now clock t4)
              (model/publish-draftset drafter test-publisher draft)

              (t/is (= expected-modifications (get-live-modification-times drafter))))))))))

(t/deftest insert-data-into-live-graph-in-new-draft-test
  (t/testing "Adding data to a live graph within a new draft with INSERT DATA should create draft modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")]

        ;; 1. live contains graph-1
        ;; 2. INSERT DATA into graph-1 within the draft
        ;;    => graph-1 should be updated within the draft modifications graph
        ;;    => draftset modified time should be updated
        ;; 3. publish draft
        ;; 4. modifications should be published to live

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph-1 (gens/generate-triples)))

          (let [draft (model/create-draftset drafter test-publisher)
                to-insert (make-quads graph-1 (gens/generate-triples))
                insert-query (util/quads->insert-data-query to-insert)]

            ;; INSERT DATA into graph-1
            (tc/set-now clock t2)
            (model/submit-update-request drafter test-publisher draft insert-query)

            (let [expected-modifications {graph-1                  t2
                                          modified-times-graph-uri t2}]

              (t/is (= expected-modifications (get-draft-modification-times drafter draft)))

              (t/is (= t2 (get-draftset-modified drafter draft)))

              (tc/set-now clock t3)
              (model/publish-draftset drafter test-publisher draft)

              (t/is (= expected-modifications (get-live-modification-times drafter))))))))))

(t/deftest insert-data-into-live-graph-in-existing-draft-test
  (t/testing "Adding data to a live graph within an existing draft with INSERT DATA should update the draft modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            t4 (.plusMinutes t3 29)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. live contains graph-1
        ;; 2. create graph-2 within a draft
        ;; 2. INSERT DATA into graph-1 within the draft
        ;;    => graph-1 should be updated within the draft modifications graph
        ;;    => draftset modified time should be updated
        ;; 3. publish draft
        ;; 4. modifications should be published to live

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph-1 (gens/generate-triples)))

          (let [draft (model/create-draftset drafter test-publisher)
                to-insert (make-quads graph-1 (gens/generate-triples))
                insert-query (util/quads->insert-data-query to-insert)]

            ;; create graph-2 within the draft
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft graph-2 (gens/generate-triples))

            ;; INSERT DATA into new graph-1
            (tc/set-now clock t3)
            (model/submit-update-request drafter test-publisher draft insert-query)

            (let [expected-modifications {graph-1                  t3
                                          graph-2                  t2
                                          modified-times-graph-uri t3}]

              (t/is (= expected-modifications (get-draft-modification-times drafter draft)))

              (t/is (= t3 (get-draftset-modified drafter draft)))

              (tc/set-now clock t4)
              (model/publish-draftset drafter test-publisher draft)

              (t/is (= expected-modifications (get-live-modification-times drafter))))))))))

;; DELETE DATA
(t/deftest delete-some-live-data-in-new-draftset-test
  (t/testing "Deleting data from live graphs within a new draft using DELETE DATA should create the draft modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")
            graph-3 (URI. "http://example.com/graph/3")]

        ;; 1. live contains graph-1, graph-2 and graph-3
        ;; 2. DELETE DATA for some of the triples in graph-1 and graph-3 in a new draft
        ;;    => draft modifications graph should contain itself, graph-1 and graph-3
        ;;    => draftset modification time should be updated
        ;; 3. publish draft
        ;;    => draft modifications should be published to live

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [graph-1-live-triples (gens/generate-triples 10)
                graph-3-live-triples (gens/generate-triples 10)
                graph-1-to-delete (take 3 graph-1-live-triples)
                graph-3-to-delete (take 6 graph-3-live-triples)
                quads-to-delete (concat (make-quads graph-1 graph-1-to-delete)
                                        (make-quads graph-3 graph-3-to-delete))]

            (publish-quads drafter (concat (make-quads graph-1 graph-1-live-triples)
                                           (make-quads graph-2 (gens/generate-triples))
                                           (make-quads graph-3 graph-3-live-triples)))

            (let [draft (model/create-draftset drafter test-publisher)
                  delete-query (util/quads->delete-data-query quads-to-delete)]

              ;; DELETE DATA for graph-1 and graph-3
              (tc/set-now clock t2)
              (model/submit-update-request drafter test-publisher draft delete-query)

              (t/is (= {graph-1 t2
                        graph-3 t2
                        modified-times-graph-uri t2} (get-draft-modification-times drafter draft)))

              (t/is (= t2 (get-draftset-modified drafter draft)))

              ;; publish draft
              (tc/set-now clock t3)
              (model/publish-draftset drafter test-publisher draft)

              ;; modification times should be updated
              (t/is (= {graph-1 t2
                        graph-2 t1
                        graph-3 t2
                        modified-times-graph-uri t2} (get-live-modification-times drafter))))))))))

(t/deftest delete-some-live-data-in-draftest-with-other-graph-changes-test
  (t/testing "Deleting data from live graphs within a new draft using DELETE DATA should create the draft modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            t4 (.plusMinutes t3 11)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. live contains graph-1
        ;; 2. create graph-2 in the draft
        ;; 3. DELETE DATA from graph-1
        ;;    => modified graph should be updated
        ;;    => draftset modified time should be updated
        ;; 4. publish draftset
        ;;    => modified times should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [graph-1-live-triples (gens/generate-triples 10)
                graph-1-to-delete (take 3 graph-1-live-triples)]

            (publish-quads drafter (make-quads graph-1 (gens/generate-triples)))

            (let [draft (model/create-draftset drafter test-publisher)]

              ;; create graph-2 in draft
              (tc/set-now clock t2)
              (append-triples drafter test-publisher draft graph-2 (gens/generate-triples))

              ;; DELETE DATA for graph-1 and
              (tc/set-now clock t3)
              (let [quads-to-delete (make-quads graph-1 graph-1-to-delete)
                    delete-query (util/quads->delete-data-query quads-to-delete)]
                (model/submit-update-request drafter test-publisher draft delete-query))

              (let [expected-modifications {graph-1                  t3
                                            graph-2                  t2
                                            modified-times-graph-uri t3}]
                (t/is (= expected-modifications (get-draft-modification-times drafter draft)))

                (t/is (= t3 (get-draftset-modified drafter draft)))

                ;; publish draft
                (tc/set-now clock t4)
                (model/publish-draftset drafter test-publisher draft)

                ;; modification times should be updated
                (t/is (= expected-modifications (get-live-modification-times drafter)))))))))))

(t/deftest delete-some-live-data-in-draftset-with-existing-graph-changes-test
  (t/testing "Updating a graph with existing changes using DELETE DATA should update the draft modified time"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            t4 (.plusMinutes t3 11)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")]

        ;; 1. live contains graph-1
        ;; 2. modify graph-1 within a draft
        ;; 3. DELETE DATA from graph-1
        ;;    => modified times should be updated
        ;;    => draftset modified time should be updated
        ;; 4. publish draftset
        ;;    => modified times should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph-1 (gens/generate-triples)))

          (let [draft (model/create-draftset drafter test-publisher)
                to-append (gens/generate-triples 10)
                to-delete (take 3 to-append)]

            ;; modify graph-1 within the draft
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft graph-1 to-append)

            ;; DELETE DATA for graph
            (tc/set-now clock t3)
            (let [quads-to-delete (make-quads graph-1 to-delete)
                  delete-query (util/quads->delete-data-query quads-to-delete)]
              (model/submit-update-request drafter test-publisher draft delete-query))

            (let [expected-modifications {graph-1                  t3
                                          modified-times-graph-uri t3}]
              (t/is (= expected-modifications (get-draft-modification-times drafter draft)))

              (t/is (= t3 (get-draftset-modified drafter draft)))

              ;; publish draft
              (tc/set-now clock t4)
              (model/publish-draftset drafter test-publisher draft)

              ;; modification times should be updated
              (t/is (= expected-modifications (get-live-modification-times drafter))))))))))

(t/deftest delete-all-live-data-in-new-draftset-test
  (t/testing "Removing all data from a live graph with DELETE DATA should update the modified times"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. live contains graph-1 and graph-2
        ;; 2. Remove contents of graph-1 within a draft with DELETE DATA
        ;;    => modified times should be updated
        ;;    => draftset modified time should be updated
        ;; 4. publish draftset
        ;;    => modified times should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [graph-1-live-quads (make-quads graph-1 (gens/generate-triples))]
            (publish-quads drafter (concat graph-1-live-quads
                                           (make-quads graph-2 (gens/generate-triples))))

            (let [draft (model/create-draftset drafter test-publisher)]

              ;; DELETE DATA for graph-1
              (tc/set-now clock t2)
              (let [delete-query (util/quads->delete-data-query graph-1-live-quads)]
                (model/submit-update-request drafter test-publisher draft delete-query))

              (t/is (= {graph-1                  t2
                        modified-times-graph-uri t2} (get-draft-modification-times drafter draft)))

              (t/is (= t2 (get-draftset-modified drafter draft)))

              ;; publish draft
              (tc/set-now clock t3)
              (model/publish-draftset drafter test-publisher draft)

              ;; modification times should be updated
              (t/is (= {graph-1 t2
                        graph-2 t1
                        modified-times-graph-uri t2} (get-live-modification-times drafter))))))))))

(t/deftest delete-all-live-data-in-draftset-with-other-graph-changes-test
  (t/testing "Removing all data from a live graph with DELETE DATA in a draft with other changes should update the modified times"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            t4 (.plusMinutes t3 5)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. live contains graph-1 and graph-2
        ;; 2. update graph-1 within a draft
        ;; 3. Remove contents of graph-2 with DELETE DATA
        ;;    => modified times should be updated
        ;;    => draftset modified time should be updated
        ;; 4. publish draftset
        ;;    => modified times should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [graph-2-live-quads (make-quads graph-2 (gens/generate-triples))]
            (publish-quads drafter (concat (make-quads graph-1 (gens/generate-triples))
                                           graph-2-live-quads))

            (let [draft (model/create-draftset drafter test-publisher)]

              ;; update graph-1
              (tc/set-now clock t2)
              (append-triples drafter test-publisher draft graph-1 (gens/generate-triples))

              ;; DELETE DATA for graph-2
              (tc/set-now clock t3)
              (let [delete-query (util/quads->delete-data-query graph-2-live-quads)]
                (model/submit-update-request drafter test-publisher draft delete-query))

              (t/is (= {graph-1                  t2
                        graph-2                  t3
                        modified-times-graph-uri t3} (get-draft-modification-times drafter draft)))

              (t/is (= t3 (get-draftset-modified drafter draft)))

              ;; publish draft
              (tc/set-now clock t4)
              (model/publish-draftset drafter test-publisher draft)

              ;; modification times should be updated
              (t/is (= {graph-1 t2
                        graph-2 t3
                        modified-times-graph-uri t3} (get-live-modification-times drafter))))))))))

(t/deftest delete-all-live-data-in-draftset-with-existing-graph-changes-test
  (t/testing "Removing all data from a live graph with DELETE DATA in a draft with existing changes to the graph should update the modified times"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            t4 (.plusMinutes t3 5)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. live contains graph-1 and graph-2
        ;; 2. update graph-1 within a draft
        ;; 3. Remove contents of graph-1 with DELETE DATA
        ;;    => modified times should be updated
        ;;    => draftset modified time should be updated
        ;; 4. publish draftset
        ;;    => modified times should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (let [graph-1-live-quads (make-quads graph-1 (gens/generate-triples))]
            (publish-quads drafter (concat graph-1-live-quads
                                           (make-quads graph-2 (gens/generate-triples))))

            (let [draft (model/create-draftset drafter test-publisher)
                  graph-1-draft-triples (gens/generate-triples)]
              ;; update graph-1
              (tc/set-now clock t2)
              (append-triples drafter test-publisher draft graph-1 graph-1-draft-triples)

              ;; DELETE DATA for graph-1
              (tc/set-now clock t3)
              (let [delete-query (util/quads->delete-data-query (concat graph-1-live-quads
                                                                   (make-quads graph-1 graph-1-draft-triples)))]
                (model/submit-update-request drafter test-publisher draft delete-query))

              (t/is (= {graph-1                  t3
                        modified-times-graph-uri t3} (get-draft-modification-times drafter draft)))

              (t/is (= t3 (get-draftset-modified drafter draft)))

              ;; publish draft
              (tc/set-now clock t4)
              (model/publish-draftset drafter test-publisher draft)

              ;; modification times should be updated
              (t/is (= {graph-1 t3
                        graph-2 t1
                        modified-times-graph-uri t3} (get-live-modification-times drafter))))))))))

(t/deftest delete-some-data-from-draft-only-graph-test
  (t/testing "Deleting some (but not all) data from a graph which exists only in a draft with DELETE DATA should update the modified time"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            t4 (.plusMinutes t3 5)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. live contains graph-1
        ;; 2. Create graph-2 within a draft
        ;; 3. Remove some of the contents of graph-2 with DELETE DATA
        ;;    => modified time should be updated
        ;;    => draftset modified time should be updated
        ;; 4. publish draftset
        ;;    => modified time should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph-1 (gens/generate-triples)))

          (let [draft (model/create-draftset drafter test-publisher)
                graph-2-draft-triples (gens/generate-triples 10)]
            ;; create graph-2
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft graph-2 graph-2-draft-triples)

            ;; DELETE DATA for graph-2
            (tc/set-now clock t3)
            (let [to-delete (take 6 graph-2-draft-triples)
                  delete-query (util/quads->delete-data-query (make-quads graph-2 to-delete))]
              (model/submit-update-request drafter test-publisher draft delete-query))

            (t/is (= {graph-2                  t3
                      modified-times-graph-uri t3} (get-draft-modification-times drafter draft)))

            (t/is (= t3 (get-draftset-modified drafter draft)))

            ;; publish draft
            (tc/set-now clock t4)
            (model/publish-draftset drafter test-publisher draft)

            ;; modification times should be updated
            (t/is (= {graph-1                  t1
                      graph-2                  t3
                      modified-times-graph-uri t3} (get-live-modification-times drafter)))))))))

(t/deftest delete-all-data-from-graph-in-draft-with-other-changes-test
  (t/testing "Removing data from a graph which exists only in a draft with other graph changes should remove it from the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            t4 (.plusMinutes t3 5)
            t5 (.plusMinutes t4 37)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. live contains graph-1
        ;; 2. new graph graph-2 is created within a draft
        ;; 3. graph-1 is modified in the draft
        ;; 4. contents of graph-2 are deleted with DELETE DATA
        ;;    => modified time for graph-2 should be removed from the modifications graph
        ;;    => draftset modified time should be updated
        ;; 5. publish draft
        ;;    => modified time for graph-1 should be updated

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph-1 (gens/generate-triples)))

          (let [draft (model/create-draftset drafter test-publisher)
                graph-2-draft-triples (gens/generate-triples 10)]

            ;; create graph-2
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft graph-2 graph-2-draft-triples)

            ;; modify graph-1
            (tc/set-now clock t3)
            (append-triples drafter test-publisher draft graph-1 (gens/generate-triples))

            ;; DELETE DATA for graph-2
            (tc/set-now clock t4)
            (let [delete-query (util/quads->delete-data-query (make-quads graph-2 graph-2-draft-triples))]
              (model/submit-update-request drafter test-publisher draft delete-query))

            (t/is (= {graph-1                  t3
                      modified-times-graph-uri t4} (get-draft-modification-times drafter draft)))

            (t/is (= t4 (get-draftset-modified drafter draft)))

            ;; publish draft
            (tc/set-now clock t5)
            (model/publish-draftset drafter test-publisher draft)

            ;; modification times should be updated
            (t/is (= {graph-1                  t3
                      modified-times-graph-uri t4} (get-live-modification-times drafter)))))))))

(t/deftest delete-all-data-from-graph-in-draft-with-no-other-changes-test
  (t/testing "Deleting all data from a graph which only exists within a draft using DELETE DATA should remove the modifications graph"
    (tc/with-system
      [:drafter/backend :drafter.fixture-data/loader]
      [system "drafter/feature/empty-db-system.edn"]
      (let [repo (get-test-repo system)
            t1 (OffsetDateTime/of 2021 1 12 10 42 51 394 ZoneOffset/UTC)
            t2 (.plusHours t1 4)
            t3 (.plusMinutes t2 43)
            t4 (.plusMinutes t3 5)
            clock (tc/manual-clock t1)
            graph-1 (URI. "http://example.com/graph/1")
            graph-2 (URI. "http://example.com/graph/2")]

        ;; 1. live contains graph-1
        ;; 2. new graph graph-2 is created within a draft
        ;; 3. contents of graph-2 are deleted with DELETE DATA
        ;;    => no visible draft changes so modified times graph should be removed
        ;;    => draftset modified time should be updated
        ;; 5. publish draft
        ;;    => modifications graph should be unchanged

        (with-open [drafter (model/make-sync (model/create repo {:clock clock}))]
          (publish-quads drafter (make-quads graph-1 (gens/generate-triples)))

          (let [initial-modification-times (get-live-modification-times drafter)
                draft (model/create-draftset drafter test-publisher)
                graph-2-draft-triples (gens/generate-triples 10)]

            ;; create graph-2
            (tc/set-now clock t2)
            (append-triples drafter test-publisher draft graph-2 graph-2-draft-triples)

            ;; DELETE DATA for graph-2
            (tc/set-now clock t3)
            (let [delete-query (util/quads->delete-data-query (make-quads graph-2 graph-2-draft-triples))]
              (model/submit-update-request drafter test-publisher draft delete-query))

            (t/is (= false (draft-modifications-graph-exists? drafter draft)))
            (t/is (= {} (get-draft-modification-times drafter draft)))

            (t/is (= t3 (get-draftset-modified drafter draft)))

            ;; publish draft
            (tc/set-now clock t4)
            (model/publish-draftset drafter test-publisher draft)

            ;; modification times should be unchanged
            (t/is (= initial-modification-times (get-live-modification-times drafter)))))))))

;; low-level functions

(defn- system-draft-repo [system live->draft]
  (backend-draftset/->RewritingSesameSparqlExecutor (get-test-repo system) live->draft false))

(defn- load-test-data [repo resource-file]
  (let [resource-path (str "drafter/feature/modified_times_test/" resource-file)]
    (fd/load-fixture! {:repo repo :format :trig :fixtures [(io/resource resource-path)]})))

;; graph modified
(t/deftest draft-graph-appended!-draft-modifications-graph-empty
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]

    ;; 1. Modifications graph is empty
    ;; 2. First batch is added to draft graph which does not exist in live
    ;;   => graph and modified graph should be updated in the draft modifications graph

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "7230c383-3899-4fb2-9c3e-099723fe39b8")
          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/5f7db244-0cad-44e3-ae3b-c4fc7c64a24b")
          draft-modified-graph (URI. "http://publishmydata.com/graphs/drafter/draft/659c02d6-2a73-4ffd-83b2-31997807a3c0")
          modified-at (OffsetDateTime/of 2021 1 7 11 05 35 0 ZoneOffset/UTC)
          live->draft {user-graph-1 user-graph-1-draft
                       modified-times-graph-uri draft-modified-graph}
          draft-repo (system-draft-repo system live->draft)]
      (load-test-data repo "draft_graph_appended_draft_modifications_graph_empty.trig")

      (sut/draft-graph-appended! repo draftset-ref draft-modified-graph user-graph-1-draft modified-at)

      (t/is (= {user-graph-1 modified-at
                modified-times-graph-uri modified-at}
               (query-modification-times draft-repo)))

      ;; draftset timestamp should be updated in the state graph
      (t/is (= modified-at (query-draftset-modified repo draftset-ref))))))

(t/deftest draft-graph-modified!-draft-graph-has-modified-time
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; Tests the scenario where:
    ;;   - the draft modifications graph is empty
    ;;   - the draft contains a draft of a graph which exists in live
    ;;   - the live graph therefore has a dcterms:issued timestamp in the state graph
    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "f54c033a-9109-41b5-aeb2-2d14e5a10e37")
          user-graph (URI. "http://example.com/graphs/1")
          user-graph-draft (URI. "http://publishmydata.com/graphs/drafter/draft/b48ebe38-e2de-44ff-a737-a34ab069157a")
          draft-modified-graph (URI. "http://publishmydata.com/graphs/drafter/draft/b56c3505-c37d-47da-bf8b-971701e0dbb6")
          modified-at (OffsetDateTime/of 2021 1 7 8 58 10 0 ZoneOffset/UTC)
          live->draft {user-graph user-graph-draft
                       modified-times-graph-uri draft-modified-graph}
          draft-repo (system-draft-repo system live->draft)]
      (load-test-data repo "draft_graph_appended_draft_graph_has_modified_time.trig")

      (sut/draft-graph-appended! repo draftset-ref draft-modified-graph user-graph-draft modified-at)

      ;; modified times of the user and modifications graph should be updated
      (t/is (= {user-graph modified-at
                modified-times-graph-uri modified-at} (query-modification-times draft-repo)))

      ;; draftset timestamp should be updated in the state graph
      (t/is (= modified-at (query-draftset-modified repo draftset-ref))))))

(t/deftest draft-graph-modified!-modifications-graph-contains-other-graph
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; 1. Draft contains graph-1 and graph-2
    ;; 2. graph-1 has previously been added to the draft so has an entry in the modifications graph
    ;; 3. data is added for graph-2
    ;;   => graph-2 should be modified in the modifications graph
    ;;   => the modifications graph last modified should be updated
    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "2bfe3b4a-1af0-4335-9a83-c2e4e9a01956")
          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/08506f99-53fd-4f52-b97d-de82a38e69a9")
          user-graph-2 (URI. "http://example.com/graphs/2")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/d9b984c1-4bfa-4614-96ce-4e432dcc0bbf")
          draft-modified-graph (URI. "http://publishmydata.com/graphs/drafter/draft/eb90dd5c-72df-4fa6-b1ce-fecb70aac7f9")
          user-graph-1-modified (OffsetDateTime/parse "2021-01-07T13:46:03.995Z")
          t2 (OffsetDateTime/parse "2021-01-07T19:58:21.483Z")
          live->draft {user-graph-1 user-graph-1-draft
                       user-graph-2 user-graph-2-draft
                       modified-times-graph-uri draft-modified-graph}
          draft-repo (system-draft-repo system live->draft)]
      (load-test-data repo "draft_graph_appended_modifications_graph_contains_other_graph.trig")

      (sut/draft-graph-appended! repo draftset-ref draft-modified-graph user-graph-2-draft t2)
      (t/is (= {user-graph-1 user-graph-1-modified
                user-graph-2 t2
                modified-times-graph-uri t2} (query-modification-times draft-repo)))

      ;; draftset modified should be updated in the state graph
      (t/is (= t2 (query-draftset-modified repo draftset-ref))))))

;; graph reverted
(t/deftest draft-graph-reverted!-last-draftset-graph-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   The user draft graph has been reverted and its draft graph removed from the draftset graph mapping
    ;;   The old draft modification graph still has a modified timestamp in the draft modifications graph
    ;;   The reverted draft graph was the last user graph in the draftset
    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "98bbbb82-77c5-45eb-bf02-e587bb00f3da")
          user-graph-draft (URI. "http://publishmydata.com/graphs/drafter/draft/08506f99-53fd-4f52-b97d-de82a38e69a9")
          t2 (OffsetDateTime/parse "2021-01-22T08:58:41.994Z")
          graph-manager (graphs/create-manager repo #{} t2)]
      (load-test-data repo "draft_graph_reverted_last_draftset_graph.trig")

      (t/is (query-draft-modifications-graph-exists? repo draftset-ref))
      (sut/draft-graph-reverted! repo graph-manager draftset-ref user-graph-draft t2)

      ;; draft modifications graph should be deleted and draftset timestamp updated
      (t/is (= false (query-draft-modifications-graph-exists? repo draftset-ref)))
      (t/is (= t2 (query-draftset-modified repo draftset-ref))))))

(t/deftest draft-graph-reverted!-other-draftset-graphs-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   draftset previously contained draft graphs for graph-1 and graph-2 (as well as the modifications graph)
    ;;   the draft for graph-1 has been reverted and removed from the draftset graph mapping
    ;;   the draft for graph-1 still has a modified timestamp in the draft modifications graph
    (let [repo (get-test-repo system)
          user-graph-2 (URI. "http://example.com/graphs/2")
          draftset-ref (draftset/->DraftsetId "cb80799f-516b-4673-aa15-a53e1d5df8ab")
          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/3d670167-52d5-41bb-a092-070e9204915f")
          user-graph-2-modified (OffsetDateTime/parse "2021-01-22T12:04:52.003Z")
          revert-time (OffsetDateTime/parse "2021-01-22T16:30:05.885Z")
          graph-manager (graphs/create-manager repo #{} revert-time)]
      (load-test-data repo "draft_graph_reverted_other_draftset_graphs.trig")

      (t/is (query-draft-modifications-graph-exists? repo draftset-ref))

      (sut/draft-graph-reverted! repo graph-manager draftset-ref user-graph-1-draft revert-time)

      ;; draft modifications graph should still exist with graph-2 timestamp
      ;; modified times graph should be modified to revert time
      (t/is (= true (query-draft-modifications-graph-exists? repo draftset-ref)))

      (let [draft-repo (backend-draftset/build-draftset-endpoint repo draftset-ref false)]
        (t/is (= {user-graph-2             user-graph-2-modified
                  modified-times-graph-uri revert-time} (query-modification-times draft-repo))))

      ;; draftset timestamp should be updated
      (t/is (= revert-time (query-draftset-modified repo draftset-ref))))))

(t/deftest draft-graph-reverted!-no-modifications-graph-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   draftset previously contained draft graphs for graph-1 and graph-2
    ;;   no draft modifications graph exists
    ;;   the draft for graph-1 has been reverted and removed from the draftset graph mapping
    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "10a4a58a-2c10-42e3-bd38-bf990aa34d78")
          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/d5673718-2b60-41b7-94e5-e9c1d79cc8dc")
          revert-time (OffsetDateTime/parse "2021-01-23T10:01:11.408Z")
          graph-manager (graphs/create-manager repo #{} revert-time)]
      (load-test-data repo "draft_graph_reverted_no_modifications_graph.trig")

      (t/is (= false (query-draft-modifications-graph-exists? repo draftset-ref)))

      (sut/draft-graph-reverted! repo graph-manager draftset-ref user-graph-1-draft revert-time)

      ;; draft modifications graph should not be created
      ;; draftset modified time should not be updated
      (t/is (= false (query-draft-modifications-graph-exists? repo draftset-ref)))
      (t/is (= revert-time (query-draftset-modified repo draftset-ref))))))

;; publish-modifications-graph
(t/deftest publish-modifications-graph-live-empty-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   no live modifications graph
    ;;   draftset contains two modified user graphs along with the modifications graph
    ;;   user-graph-1 was modified at t1
    ;;   user-graph-2, draftset and modifications graph all last modified at t2
    ;;   publication occurs at t3
    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "07cc00b4-137f-4587-a5af-e0edda6c6c9f")
          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-2 (URI. "http://example.com/graphs/2")
          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/0cc7983a-f0d7-4517-a38a-352226007d49")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/762d6075-5c3a-4813-b69e-9aa4c26718c0")
          modifications-graph-draft (URI. "http://publishmydata.com/graphs/drafter/draft/8a5b00e9-ad0d-4c1b-84de-39d5dff57c9f")
          live->draft {user-graph-1 user-graph-1-draft
                       user-graph-2 user-graph-2-draft
                       modified-times-graph-uri modifications-graph-draft}

          t1 (OffsetDateTime/parse "2021-01-23T17:34:21.058Z")
          t2 (OffsetDateTime/parse "2021-01-24T16:11:30.936Z")
          t3 (.plusMinutes t2 21)]
      (load-test-data repo "publish_modifications_graph_live_empty.trig")

      (t/is (= true (query-draft-modifications-graph-exists? repo draftset-ref)))

      (sut/publish-modifications-graph repo live->draft t3)

      ;; draft modifications graph should be removed
      (t/is (= true (mgmt/graph-empty? repo modifications-graph-draft)))
      (t/is (= false (mgmt-helpers/draft-exists? repo modifications-graph-draft)))

      (with-open [drafter (model/create repo)]
        (t/is (live-modifications-graph-exists? drafter))
        (t/is (= {user-graph-1             t1
                  user-graph-2             t2
                  modified-times-graph-uri t2} (get-live-modification-times drafter)))))))

(t/deftest publish-modifications-graph-new-draft-graphs-only-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   live modifications graph exists containing graph-1 (and the modifications graph itself)
    ;;   draftset contains two new graphs: graph-2 and graph-3
    ;;   live modifications graph and live graph-1 modified at t1
    ;;   draft graph graph-3 modified at t2 and draft modifications graph and graph-2 modified at t3
    ;;   publish occurs at t4
    ;;     => live modification time for graph-1 should be unaffected
    ;;     => graph-2 should be included in live at t3
    ;;     => live modifications graph should be updated to t3
    ;;     => graph-3 should be included in live at t2
    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "bf83a7d2-c4e4-4453-ba69-28c502c74529")
          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-2 (URI. "http://example.com/graphs/2")
          user-graph-3 (URI. "http://example.com/graphs/3")

          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/7d9b46b5-d00f-4f99-a5f8-1c4c2cef8b29")
          user-graph-3-draft (URI. "http://publishmydata.com/graphs/drafter/draft/0cbef5c2-959f-4b1a-9737-d5efeb675094")
          modifications-graph-draft (URI. "http://publishmydata.com/graphs/drafter/draft/2231eb8c-718b-4c84-933a-50941badf91c")
          live->draft {user-graph-2 user-graph-2-draft
                       user-graph-3 user-graph-3-draft
                       modified-times-graph-uri modifications-graph-draft}

          t1 (OffsetDateTime/parse "2021-01-25T09:34:20.844Z")
          t2 (OffsetDateTime/parse "2021-01-25T10:01:43.219Z")
          t3 (OffsetDateTime/parse "2021-01-25T16:53:05.554Z")
          t4 (.plusMinutes t3 46)]

      (load-test-data repo "publish_modifications_graph_new_draft_graphs_only.trig")

      (with-open [drafter (model/create repo {:clock t4})]

        (t/is (= true (live-modifications-graph-exists? drafter)))
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))

        (sut/publish-modifications-graph repo live->draft t4)

        ;; draft modifications graph should be removed
        (t/is (= true (mgmt/graph-empty? repo modifications-graph-draft)))
        (t/is (= false (mgmt-helpers/draft-exists? repo modifications-graph-draft)))

        (t/is (live-modifications-graph-exists? drafter))
        (t/is (= false (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1             t1
                  user-graph-2             t3
                  user-graph-3             t2
                  modified-times-graph-uri t3} (get-live-modification-times drafter)))))))

(t/deftest publish-modifications-graph-later-draft-modifications-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   live modifications graph exists containing graph-1 and graph-2 (and the modifications graph itself)
    ;;   draftset contains changes to both live graphs
    ;;   live graph-1 is modified at t1
    ;;   live graph-2 and modifications graph are modified at t2
    ;;   draft graph-2 is modified at t3
    ;;   draft graph-1 and modifications graphs are modified at t4
    ;;   publish occurs at t5
    ;;     => live modification time of graph-1 should be updated to t4
    ;;     => live modification time of graph-2 should be updated to t3
    ;;     => live modification time of the modifications graph should be updated to t4
    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "45e376e8-1cc6-4dc1-9f4e-703b16d5b842")
          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-2 (URI. "http://example.com/graphs/2")

          modifications-graph-draft (URI. "http://publishmydata.com/graphs/drafter/draft/ab885f9c-14db-47eb-870a-0e6846e69846")
          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/2d60455d-00f9-4644-a986-b0b2fac22ec9")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/d48a0852-cff8-4f3a-91b1-2ed3a58c3df8")

          live->draft {user-graph-1 user-graph-1-draft
                       user-graph-2 user-graph-2-draft
                       modified-times-graph-uri modifications-graph-draft}

          t1 (OffsetDateTime/parse "2021-01-25T09:34:20.844Z")
          t2 (OffsetDateTime/parse "2021-01-25T10:01:43.219Z")
          t3 (OffsetDateTime/parse "2021-01-25T16:53:05.554Z")
          t4 (OffsetDateTime/parse "2021-01-25T17:04:52.577Z")
          t5 (.plusMinutes t4 46)]

      (load-test-data repo "publish_modifications_graph_later_draft_modifications.trig")

      (with-open [drafter (model/create repo {:clock t4})]

        (t/is (= true (live-modifications-graph-exists? drafter)))
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1 t1
                  user-graph-2 t2
                  modified-times-graph-uri t2} (get-live-modification-times drafter)))

        (sut/publish-modifications-graph repo live->draft t5)

        ;; draft modifications graph should be removed
        (t/is (= true (mgmt/graph-empty? repo modifications-graph-draft)))
        (t/is (= false (mgmt-helpers/draft-exists? repo modifications-graph-draft)))

        (t/is (live-modifications-graph-exists? drafter))
        (t/is (= false (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1             t4
                  user-graph-2             t3
                  modified-times-graph-uri t4} (get-live-modification-times drafter)))))))

(t/deftest publish-modifications-graph-new-and-live-modification-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   live modifications graph exists containing graph-1 (and the modifications graph itself)
    ;;   draftset contains a modification to graph-1 and a new graph graph-2
    ;;   live modifications graph and live graph-1 modified at t1
    ;;   draft of graph-1 and the modifications graph are modified at t3
    ;;   draft of new graph graph-2 is modified at t2
    ;;   publish occurs at t4
    ;;     => live modification time for graph-1 should be updated to t3
    ;;     => graph-2 should be included in live at t2
    ;;     => live modifications graph should be updated to t3
    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "a8e624ed-1b83-447b-820c-44768b1f8991")
          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-2 (URI. "http://example.com/graphs/2")

          modifications-graph-draft (URI. "http://publishmydata.com/graphs/drafter/draft/fd18120a-a899-453a-905d-40fe881f7da8")
          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/c6fc832b-0ad7-4803-9160-0dae3ec43863")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/f2734b1c-0e65-4b26-a7c7-056f3237ed12")

          live->draft {user-graph-1 user-graph-1-draft
                       user-graph-2 user-graph-2-draft
                       modified-times-graph-uri modifications-graph-draft}

          t1 (OffsetDateTime/parse "2021-01-25T09:34:20.844Z")
          t2 (OffsetDateTime/parse "2021-01-25T10:01:43.219Z")
          t3 (OffsetDateTime/parse "2021-01-25T16:53:05.554Z")
          t4 (.plusMinutes t3 46)]

      (load-test-data repo "publish_modifications_graph_new_and_live_modification.trig")

      (with-open [drafter (model/create repo {:clock t4})]

        (t/is (= true (live-modifications-graph-exists? drafter)))
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1 t1
                  modified-times-graph-uri t1} (get-live-modification-times drafter)))

        (sut/publish-modifications-graph repo live->draft t4)

        ;; draft modifications graph should be removed
        (t/is (= true (mgmt/graph-empty? repo modifications-graph-draft)))
        (t/is (= false (mgmt-helpers/draft-exists? repo modifications-graph-draft)))

        (t/is (live-modifications-graph-exists? drafter))
        (t/is (= false (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1             t3
                  user-graph-2             t2
                  modified-times-graph-uri t3} (get-live-modification-times drafter)))))))

(t/deftest publish-modifications-graph-earlier-draft-modifications-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   live modifications graph exists containing graph-1, graph-2 and graph-3 (and the modifications graph itself)
    ;;   draftset contains a modifications for graph-1 and graph-2
    ;;   draftset modification for graph-1 is earlier than the live modification (and for the modifications graph)
    ;;   draftset modification for graph-2 is later than the modification in live
    ;;   live modification of graph-2 at t1
    ;;   live modification of graph-3 at t2
    ;;   draft modification of graph-2 at t3
    ;;   draft modification of graph-1 and modification graph at t4
    ;;   live modification of graph-1 and modification graph at t5
    ;;   publish at t6
    ;;     => modification for graph-1 and the modifications graph should be the publish time t6
    ;;     => modification for graph-2 should be updated to t3 as in the draft
    ;;     => modification for graph-3 should be unaffected
    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "13604fed-3971-4c93-85b0-04081490d637")
          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-2 (URI. "http://example.com/graphs/2")
          user-graph-3 (URI. "http://example.com/graphs/3")

          modifications-graph-draft (URI. "http://publishmydata.com/graphs/drafter/draft/53b38e87-49a8-42a5-99b0-62375dbfdee7")
          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/ffb1bfbd-5340-41d3-991d-f99ccf1c5c11")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/e9e7f199-2f20-451d-ba2f-b027aa6c8ee5")

          live->draft {user-graph-1 user-graph-1-draft
                       user-graph-2 user-graph-2-draft
                       modified-times-graph-uri modifications-graph-draft}

          t1 (OffsetDateTime/parse "2021-01-25T09:34:20.844Z")
          t2 (OffsetDateTime/parse "2021-01-25T10:01:43.219Z")
          t3 (OffsetDateTime/parse "2021-01-25T16:53:05.554Z")
          t4 (OffsetDateTime/parse "2021-01-25T17:04:11.405Z")
          t5 (OffsetDateTime/parse "2021-01-26T10:46:30.442Z")
          t6 (OffsetDateTime/parse "2021-01-26T13:44:39.942Z")]

      (load-test-data repo "publish_modifications_graph_earlier_draft_modifications.trig")

      (with-open [drafter (model/create repo {:clock t6})]

        (t/is (= true (live-modifications-graph-exists? drafter)))
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1 t5
                  user-graph-2 t1
                  user-graph-3 t2
                  modified-times-graph-uri t5} (get-live-modification-times drafter)))

        (sut/publish-modifications-graph repo live->draft t6)

        ;; draft modifications graph should be removed
        (t/is (= true (mgmt/graph-empty? repo modifications-graph-draft)))
        (t/is (= false (mgmt-helpers/draft-exists? repo modifications-graph-draft)))

        (t/is (live-modifications-graph-exists? drafter))
        (t/is (= false (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1 t6
                  user-graph-2 t3
                  user-graph-3 t2
                  modified-times-graph-uri t6} (get-live-modification-times drafter)))))))

(defn- apply-update-modifications-queries [repo graph-manager draftset-ref draft-graph-uris updated-at]
  (let [queries (sut/update-modifications-queries repo graph-manager draftset-ref draft-graph-uris updated-at)
        q (jena/->update-string queries)]
    (with-open [conn (repo/->connection repo)]
      (sparql/update! conn q))))

;; update-modifications-queries
(t/deftest update-modifications-queries-draft-graphs-created-no-live-modifications-graph-exists-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   draft graphs for graph-1 and graph-2 have been created within a previously-empty draftset
    ;;   no draft modifications graph exists
    ;;   => modifications graph should be created containing itself and new graphs
    ;;   => draftset timestamp should be updated

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "c40d5ac0-5565-44e4-ba6f-fa8a9704bb1e")
          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-2 (URI. "http://example.com/graphs/2")

          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/88d0e605-15a6-44d3-b3d5-3d080bfc8e03")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/320b4f2f-9531-4aea-a185-dde29b19e21b")

          updated-at (OffsetDateTime/parse "2021-01-27T09:53:02.590Z")
          graph-manager (graphs/create-manager repo #{} updated-at)]

      (load-test-data repo "update_modifications_queries_draft_graphs_created_no_live_modifications_graph_exists.trig")

      (apply-update-modifications-queries repo graph-manager draftset-ref [user-graph-1-draft user-graph-2-draft] updated-at)

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1 updated-at
                  user-graph-2 updated-at
                  modified-times-graph-uri updated-at} (get-draft-modification-times drafter draftset-ref)))
        (t/is (= updated-at (get-draftset-modified drafter draftset-ref)))

        ;; modifications graph should not be public
        (t/is (= :draft (tc/get-graph-state repo modified-times-graph-uri)))))))

(t/deftest update-modifications-queries-created-empty-graph-no-live-modifications-graph-exists-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   draft graphs for graph-1 and graph-2 have been created within a previously-empty draftset
    ;;   neither graph contains data after the update(s)
    ;;   no draft modifications graph exists
    ;;   => no visible draft changes so modifications graph should not be created

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "5f5cbb4c-2f92-4edb-98a0-5a5b8a9b103f")

          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/1848ae4e-fc94-44fd-ac5d-cb021471d621")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/eca787f2-ee94-4581-87fb-726f36e48dea")

          updated-at (OffsetDateTime/parse "2021-01-27T09:53:02.590Z")
          graph-manager (graphs/create-manager repo #{} updated-at)]

      (load-test-data repo "update_modifications_queries_created_empty_graphs_no_live_modifications_graph_exists.trig")

      (apply-update-modifications-queries repo graph-manager draftset-ref [user-graph-1-draft user-graph-2-draft] updated-at)

      (with-open [drafter (model/create repo)]
        (t/is (= false (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= updated-at (get-draftset-modified drafter draftset-ref)))

        (t/is (= :draft (tc/get-graph-state repo modified-times-graph-uri)))))))

(t/deftest update-modifications-queries-created-empty-graphs-no-live-modifications-graph-exists-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   draft graphs for graph-1 and graph-2 have been created within a previously-empty draftset
    ;;   graph-1 contains data after the update(s) but graph-2 is empty
    ;;   no draft modifications graph exists
    ;;   => modifications graph should be created containing itself and graph-1
    ;;   => graph-2 should not be included

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "54e66ecb-fc6e-4faa-a4a4-80df96b4ca1c")
          user-graph-1 (URI. "http://example.com/graphs/1")

          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/16c4668e-2c7f-4399-af8f-b1b0bd36411f")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/950ca565-bacf-4fd4-a830-7485d07fecc4")

          updated-at (OffsetDateTime/parse "2021-01-27T09:53:02.590Z")
          graph-manager (graphs/create-manager repo #{} updated-at)]

      (load-test-data repo "update_modifications_queries_created_empty_graph_no_live_modifications_graph_exists.trig")

      (apply-update-modifications-queries repo graph-manager draftset-ref [user-graph-1-draft user-graph-2-draft] updated-at)

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1             updated-at
                  modified-times-graph-uri updated-at} (get-draft-modification-times drafter draftset-ref)))

        (t/is (= updated-at (get-draftset-modified drafter draftset-ref)))

        ;; modifications graph should not be public
        (t/is (= :draft (tc/get-graph-state repo modified-times-graph-uri)))))))

(t/deftest update-modifications-queries-modified-live-graph-no-draft-modifications-graph-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   graph-1 and modifications graph exist in live
    ;;   graph-1 is modified within a draft
    ;;   no draft modifications graph exists
    ;;   => draft modifications graph should be created containing itself and graph-1
    ;;   => modifications managed graph should not be re-created

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "8b872324-721e-4f6c-acd3-d50045482c2f")
          user-graph-1 (URI. "http://example.com/graphs/1")

          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/987d49a1-e030-40f7-9522-4938af46e407")

          updated-at (OffsetDateTime/parse "2021-01-27T09:53:02.590Z")
          graph-manager (graphs/create-manager repo #{} updated-at)]

      (load-test-data repo "update_modifications_queries_modified_live_graph_no_draft_modifications_graph.trig")

      (apply-update-modifications-queries repo graph-manager draftset-ref [user-graph-1-draft] updated-at)

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1 updated-at
                  modified-times-graph-uri updated-at} (get-draft-modification-times drafter draftset-ref)))
        (t/is (= updated-at (get-draftset-modified drafter draftset-ref)))

        ;; managed modifications graph should still be in live
        (t/is (= :live (tc/get-graph-state repo modified-times-graph-uri)))))))

(t/deftest update-modifications-queries-deleted-live-graph-no-draft-modifications-graph-exists-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   graph-1 and modifications graph exist in live
    ;;   graph-1 is deleted within a draft
    ;;   no draft modifications graph exists
    ;;   => draft modifications graph should be created containing itself and graph-1
    ;;   => modifications managed graph should not be re-created

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "81d32692-feeb-4de6-97b8-6beb8d5f0bc5")
          user-graph-1 (URI. "http://example.com/graphs/1")

          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/2af87107-33f9-40bd-9e57-cfd43797ac00")

          updated-at (OffsetDateTime/parse "2021-01-27T09:53:02.590Z")
          graph-manager (graphs/create-manager repo #{} updated-at)]

      (load-test-data repo "update_modifications_queries_deleted_live_graph_no_draft_modifications_graph_exists.trig")

      (apply-update-modifications-queries repo graph-manager draftset-ref [user-graph-1-draft] updated-at)

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1             updated-at
                  modified-times-graph-uri updated-at} (get-draft-modification-times drafter draftset-ref)))
        (t/is (= updated-at (get-draftset-modified drafter draftset-ref)))

        ;; managed modifications graph should still be in live
        (t/is (= :live (tc/get-graph-state repo modified-times-graph-uri)))))))

(t/deftest update-modifications-queries-created-new-graphs-live-modifications-graph-exists-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   graph-1 and modifications graph exist in live
    ;;   graph-1 is modified and graph-2 is created within a draft
    ;;   no draft modifications graph exists
    ;;   => draft modifications graph should be created containing itself, graph-1 and graph-2
    ;;   => modifications managed graph should not be re-created

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "10c7dff7-67cb-4280-982a-2337aa24df88")
          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-2 (URI. "http://example.com/graphs/2")

          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/386bbe73-6da6-4b03-8bdd-4d71a6742f94")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/b3c92ab9-17df-488b-92f8-596e418f80c6")

          updated-at (OffsetDateTime/parse "2021-01-27T09:53:02.590Z")
          graph-manager (graphs/create-manager repo #{} updated-at)]

      (load-test-data repo "update_modifications_queries_created_new_graphs_live_modifications_graph_exists.trig")

      (apply-update-modifications-queries repo graph-manager draftset-ref [user-graph-1-draft user-graph-2-draft] updated-at)

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1             updated-at
                  user-graph-2             updated-at
                  modified-times-graph-uri updated-at} (get-draft-modification-times drafter draftset-ref)))
        (t/is (= updated-at (get-draftset-modified drafter draftset-ref)))

        ;; managed modifications graph should still be in live
        (t/is (= :live (tc/get-graph-state repo modified-times-graph-uri)))))))

(t/deftest update-modifications-queries-draft-graph-modified-draft-modifications-graph-exists-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   graph-1 and graph-2 exist within a draft
    ;;   draft modifications graph exists
    ;;   graph-2 is updated
    ;;   => graph-2 and modifications graph modified times should be updated
    ;;   => graph-1 should be unchanged

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "ff5cdb64-7911-48ac-afc5-deeec223c940")
          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-2 (URI. "http://example.com/graphs/2")

          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/ac93f5d3-cea0-41c9-91f5-57aa6e1fe93b")

          graph-1-modified (OffsetDateTime/parse "2021-02-02T12:03:45.094Z")
          updated-at (OffsetDateTime/parse "2021-02-04T15:22:02.005Z")
          graph-manager (graphs/create-manager repo #{} updated-at)]

      (load-test-data repo "update_modifications_queries_draft_graph_modified_draft_modifications_graph_exists.trig")

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref))))

      (apply-update-modifications-queries repo graph-manager draftset-ref [user-graph-2-draft] updated-at)

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1             graph-1-modified
                  user-graph-2             updated-at
                  modified-times-graph-uri updated-at} (get-draft-modification-times drafter draftset-ref)))
        (t/is (= updated-at (get-draftset-modified drafter draftset-ref)))

        ;; managed modifications graph should still be unpublished
        (t/is (= :draft (tc/get-graph-state repo modified-times-graph-uri)))))))

(t/deftest update-modifications-queries-draft-only-graph-deleted-other-changes-exist-draft-modifications-graph-exists-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   graph-1 and graph-2 exist only within a draft
    ;;   draft modifications graph exists
    ;;   graph-1 data is deleted by an update
    ;;   => graph-1 should be removed from the draft modifications graph
    ;;   => modifications graph modified time should be updated
    ;;   => graph-2 should be unchanged

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "a1016b94-2b95-48bb-8049-7bfb7cb80cf8")
          user-graph-2 (URI. "http://example.com/graphs/2")

          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/6ce2c173-c671-4400-bd77-058f2a657b2d")

          graph-2-modified-at (OffsetDateTime/parse "2021-02-02T08:34:01.228Z")
          updated-at (OffsetDateTime/parse "2021-02-04T15:22:02.005Z")
          graph-manager (graphs/create-manager repo #{} updated-at)]

      (load-test-data repo "update_modifications_queries_draft_only_graph_deleted_other_changes_exist_draft_modifications_graph_exists.trig")

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref))))

      (apply-update-modifications-queries repo graph-manager draftset-ref [user-graph-1-draft] updated-at)

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-2             graph-2-modified-at
                  modified-times-graph-uri updated-at} (get-draft-modification-times drafter draftset-ref)))
        (t/is (= updated-at (get-draftset-modified drafter draftset-ref)))

        ;; managed modifications graph should still be unpublished
        (t/is (= :draft (tc/get-graph-state repo modified-times-graph-uri)))))))

(t/deftest update-modifications-queries-draft-only-graphs-deleted-no-other-changes-exist-draft-modifications-graph-exists-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   graph-1 and graph-2 exist only within a draft
    ;;   draft modifications graph exists
    ;;   graph-1 and graph-2 data both deleted by an update
    ;;   => no visible draft changes so draft modifications graph should be deleted

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "e579bd53-a23b-4490-a6f2-e5643245a2fb")

          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/a6cca423-99d1-4420-9665-cc0704da9355")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/30127a9f-eb3a-4db5-b598-c033767a1f61")

          updated-at (OffsetDateTime/parse "2021-02-04T15:22:02.005Z")
          graph-manager (graphs/create-manager repo #{} updated-at)]

      (load-test-data repo "update_modifications_queries_draft_only_graphs_deleted_no_other_changes_exist_draft_modifications_graph_exists.trig")

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref))))

      (apply-update-modifications-queries repo graph-manager draftset-ref [user-graph-1-draft user-graph-2-draft] updated-at)

      (with-open [drafter (model/create repo)]
        (t/is (= false (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {} (get-draft-modification-times drafter draftset-ref)))
        (t/is (= updated-at (get-draftset-modified drafter draftset-ref)))

        ;; managed modifications graph should still be unpublished
        (t/is (= :draft (tc/get-graph-state repo modified-times-graph-uri)))))))

(t/deftest update-modifications-queries-live-graph-deleted-other-changes-exist-draft-modifications-graph-exists-test
  (tc/with-system
    [:drafter.fixture-data/loader :drafter/backend]
    [system "drafter/feature/empty-db-system.edn"]
    ;; scenario where:
    ;;   graph-1 and graph-2 exist in live
    ;;   graph-3 exists only in the draft
    ;;   graph-1 is deleted and graph-2 modified within a draft
    ;;   => graph-1, graph-2 and the modifications graph should be updated
    ;;   => graph-3 should be unchanged

    (let [repo (get-test-repo system)
          draftset-ref (draftset/->DraftsetId "1e9e6722-b941-48a4-baac-6cea91405b73")

          user-graph-1 (URI. "http://example.com/graphs/1")
          user-graph-2 (URI. "http://example.com/graphs/2")
          user-graph-3 (URI. "http://example.com/graphs/3")

          user-graph-1-draft (URI. "http://publishmydata.com/graphs/drafter/draft/8f32682a-ba04-4305-8abf-63295a5dd0de")
          user-graph-2-draft (URI. "http://publishmydata.com/graphs/drafter/draft/f61c7498-3621-48c9-bfce-134c160c391f")

          graph-3-modified-at (OffsetDateTime/parse "2021-02-01T11:04:57.898Z")
          updated-at (OffsetDateTime/parse "2021-02-04T15:22:02.005Z")
          graph-manager (graphs/create-manager repo #{} updated-at)]

      (load-test-data repo "update_modifications_queries_live_graph_deleted_other_changes_exist_draft_modifications_graph_exists.trig")

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref))))

      (apply-update-modifications-queries repo graph-manager draftset-ref [user-graph-1-draft user-graph-2-draft] updated-at)

      (with-open [drafter (model/create repo)]
        (t/is (= true (draft-modifications-graph-exists? drafter draftset-ref)))
        (t/is (= {user-graph-1 updated-at
                  user-graph-2 updated-at
                  user-graph-3 graph-3-modified-at
                  modified-times-graph-uri updated-at} (get-draft-modification-times drafter draftset-ref)))
        (t/is (= updated-at (get-draftset-modified drafter draftset-ref)))

        ;; managed modifications graph should still be live
        (t/is (= :live (tc/get-graph-state repo modified-times-graph-uri)))))))
