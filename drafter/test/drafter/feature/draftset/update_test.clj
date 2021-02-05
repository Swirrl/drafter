(ns ^:rest-api drafter.feature.draftset.update-test
  (:require
   [clojure.set :as set]
   [clojure.string :as string]
   [clojure.test :as t :refer [is testing]]
   [clojure.test.check.generators :as gen]
   [drafter.backend.draftset.draft-management :as mgmt]
   [drafter.backend.draftset.operations :as dsops]
   [drafter.draftset :as ds]
   [drafter.feature.draftset.query-test :refer [create-query-request]]
   [drafter.feature.draftset.test-helper :as help]
   [drafter.feature.draftset.update :as update]
   [drafter.feature.middleware :as middleware]
   [drafter.rdf.jena :as jena]
   [drafter.rdf.sparql :as sparql]
   [drafter.test-common :as tc]
   [drafter.test-helpers.draft-management-helpers :as mgmt-helpers]
   [drafter.time :as time]
   [drafter.user-test :refer [test-editor test-publisher]]
   [grafter-2.rdf.protocols :as pr]
   [grafter-2.rdf4j.repository :as repo])
  (:import
   java.net.URI
   java.time.Duration
   java.util.UUID))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "drafter/feature/empty-db-system.edn")
(def manual-clock-config "drafter/feature/empty-db-system-manual-clock.edn")

(def keys-for-test
  [[:drafter/routes :draftset/api]
   :drafter/backend
   :drafter/write-scheduler
   :drafter.routes.sparql/live-sparql-query-route
   :drafter.backend.live/endpoint
   :drafter.common.config/sparql-query-endpoint
   :drafter.fixture-data/loader])

(defn- create-update-request
  [user draftset-location stmt]
  (tc/with-identity user
    {:uri (str draftset-location "/update")
     :headers {"accept" "text/plain"
               "content-type" "application/sparql-update"}
     :request-method :post
     :body stmt}))

(def graph-gen (gen/fmap (fn [id]
                           (URI. (str "http://g/" id))) gen/uuid))

(def subject-gen (gen/fmap (fn [id]
                             (URI. (str "http://s/" id))) gen/uuid))

(def predicate-gen (gen/fmap (fn [id]
                               (URI. (str "http://p/" id))) gen/uuid))

(def object-uri-gen (gen/fmap (fn [id]
                                (URI. (str "http://o/" id))) gen/uuid))

(def object-gen (gen/one-of [object-uri-gen
                             gen/string-alphanumeric
                             gen/int
                             gen/boolean]))

(defn quad-gen
  ([] (quad-gen {}))
  ([gens]
   (let [default-gens {:s subject-gen
                       :p predicate-gen
                       :o object-gen
                       :c graph-gen}
         gens (merge default-gens gens)]
     (gen/fmap pr/map->Quad (apply gen/hash-map (mapcat identity gens))))))

(defn- random-graph-uri []
  (gen/generate graph-gen))

(defn- submit-update
  "Submit an update request within a draftset and return the response map"
  [handler user draftset-location query]
  (let [req (create-update-request user draftset-location query)]
    (handler req)))

(defn- apply-update
  "Submits an update query as a user within a draftset and asserts it applied successfully"
  [handler user draftset-location query]
  (let [resp (submit-update handler user draftset-location query)]
    (tc/assert-is-no-content-response resp)
    nil))

(defn- generate-graph-triples
  "Generates the specified number of triples within a graph"
  [graph-uri n]
  (let [graph-gen (gen/return graph-uri)
        qg (quad-gen {:c graph-gen
                      ;; NOTE: generate some self-referential objects which are subject to rewriting
                      :o (gen/one-of [graph-gen object-gen])})]
    (gen/generate (gen/vector qg n))))

(defn- generate-quads
  "Generates the specified number of random quads"
  [n]
  (let [graph-uri (random-graph-uri)]
    (generate-graph-triples graph-uri n)))

(defn- get-graph-state
  "Returns the state of the graph in the state graph:
     :unmanaged - the graph is not a managed graph
     :draft     - the graph is managed but exists only within drafts
     :public    - the graph is live"
  [repo graph-uri]
  (let [q (str
            "PREFIX drafter: <http://publishmydata.com/def/drafter/>"
            "SELECT ?public WHERE {"
            "  GRAPH <http://publishmydata.com/graphs/drafter/drafts> {"
            "    <" graph-uri "> drafter:isPublic ?public ."
            "  }"
            "}")
        state-mapping {nil :unmanaged
                       true :live
                       false :draft}
        state (:public (sparql/select-1 repo q))]
    (get state-mapping state)))

;; NOTE: This is the max update operation size defined in web.edn
(def max-update-size 50)

(t/deftest insert-data-test
  (tc/with-system keys-for-test
    [system system-config]
    (let [repo (:drafter/backend system)
          handler (get system [:drafter/routes :draftset/api])
          draftset-location (help/create-draftset-through-api handler test-publisher)
          graph-uri (random-graph-uri)
          draft-graph-quads (generate-graph-triples graph-uri 2)
          insert-query (jena/->update-string [(jena/insert-data-stmt draft-graph-quads)])]
      (apply-update handler test-publisher draftset-location insert-query)
      (let [graph-triples (help/get-draftset-graph-triples repo draftset-location graph-uri)
            expected (set (map pr/map->Triple draft-graph-quads))]
        (t/is (= :draft (get-graph-state repo graph-uri)))
        (t/is (= expected graph-triples))))))

(t/deftest insert-modify-test
  (tc/with-system
    keys-for-test [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])
          repo (:drafter/backend system)
          draftset-location (help/create-draftset-through-api handler test-editor)
          g (random-graph-uri)]
      (let [stmt (format " INSERT DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } " g)]
        (apply-update handler test-editor draftset-location stmt)
        (t/is (= :draft (get-graph-state repo g))))

      (let [stmt (format "
PREFIX test: <http://test/>
DELETE DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } ;
INSERT DATA { GRAPH <%s> { <http://s> <http://p> <%s> } }
" g g g)]
        (apply-update handler test-editor draftset-location stmt))

      (let [expected #{(pr/->Quad (URI. "http://s") (URI. "http://p") g g)}
            draftset-quads (help/get-draftset-quads repo draftset-location)]
        (t/is (= expected draftset-quads))
        (t/is (= :draft (get-graph-state repo g)))))))

(t/deftest insert-data-into-live-graph-test
  (t/testing "INSERT DATA into a live graph causes a draft graph to be created and the live graph data cloned"
    (tc/with-system
      keys-for-test [system system-config]
      (let [repo (:drafter/backend system)
            handler (get system [:drafter/routes :draftset/api])
            g (random-graph-uri)
            live (generate-graph-triples g 20)
            new-in-draft (generate-graph-triples g 10)]
        (help/publish-quads-through-api handler live)

        (let [draftset-location (help/create-draftset-through-api handler test-publisher)
              insert-query (jena/->update-string [(jena/insert-data-stmt new-in-draft)])]
          (apply-update handler test-publisher draftset-location insert-query)

          (let [expected (set (map pr/map->Triple (concat live new-in-draft)))
                graph-triples (help/get-draftset-graph-triples repo draftset-location g)]
            (t/is (= expected (set graph-triples)))

            ;; graph should still be live
            (t/is (= :live (get-graph-state repo g)))))))))

(t/deftest prefix-mapping-rewrite-test
  (tc/with-system
    ;; Same test as above, but use a prefix mapping - actually works for free
    keys-for-test [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])
          repo (:drafter/backend system)
          draftset-location (help/create-draftset-through-api handler test-editor)
          g (random-graph-uri)]

      (let [stmt (format " INSERT DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } " g)]
        (apply-update handler test-editor draftset-location stmt))

      (let [stmt (format "
PREFIX d: <%s>
DELETE DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } ;
INSERT DATA { GRAPH <%s> { <http://s> <http://p> d: } }
" g g g)]
        (apply-update handler test-editor draftset-location stmt))

      (let [expected #{(pr/->Quad (URI. "http://s") (URI. "http://p") g g)}
            draftset-quads (help/get-draftset-quads repo draftset-location)]
        (t/is (= expected draftset-quads))))))

(t/deftest base-uri-rewrite-test
  (tc/with-system
    ;; Same test as above, but use a base URI - actually works for free
    keys-for-test [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])
          draftset-location (help/create-draftset-through-api handler test-editor)
          repo (:drafter/backend system)
          uuid (UUID/randomUUID)
          g (URI. (str "http://g/" uuid))]

      (let [stmt (format " INSERT DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } " g)]
        (apply-update handler test-editor draftset-location stmt))

      (let [stmt (format "
BASE <http://g/>
PREFIX d: <%s>
DELETE DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } ;
INSERT DATA { GRAPH <%s> { <http://s> <http://p> d: } }
" uuid uuid uuid)]
        (apply-update handler test-editor draftset-location stmt))

      (let [expected #{(pr/->Quad (URI. "http://s") (URI. "http://p") g g)}
            draftset-quads (help/get-draftset-quads repo draftset-location)]
        (t/is (= expected draftset-quads))))))

(t/deftest insert-and-delete-max-payload-test
  (tc/with-system
    keys-for-test [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])
          repo (:drafter/backend system)]
      (testing "Maximum size of payload"
        (testing "Insert max payload"
          (let [draftset-location (help/create-draftset-through-api handler test-editor)
                quads (set (generate-quads max-update-size))
                stmt (jena/->update-string [(jena/insert-data-stmt quads)])]
            (apply-update handler test-editor draftset-location stmt)
            (let [draftset-quads (help/get-draftset-quads repo draftset-location)]
              (t/is (= quads draftset-quads)))

            (testing "Delete max payload"
              (let [delete-query (jena/->update-string [(jena/delete-data-stmt quads)])]
                (apply-update handler test-editor draftset-location delete-query))
              (t/is (= #{} (help/get-draftset-quads repo draftset-location)))))))

      (testing "Too large payload"
        (testing "Insert too large payload"
          (let [draftset-location (help/create-draftset-through-api handler test-editor)
                quads (generate-quads (inc max-update-size))
                stmt (jena/->update-string [(jena/insert-data-stmt quads)])
                response (submit-update handler test-editor draftset-location stmt)]
            (t/testing "Delete too large payload"
              (tc/assert-is-payload-too-large-response response))
            (t/testing "Insert failed"
              (t/is (= #{} (help/get-draftset-quads repo draftset-location))))))))))

(t/deftest DELETE_INSERT-max-payload-test
  (tc/with-system
    keys-for-test [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])
          repo (:drafter/backend system)]
      (testing "DELETE/INSERT max payload"
        (let [draftset-location (help/create-draftset-through-api handler test-editor)
              n 25
              quads1 (generate-quads n)
              quads2 (generate-quads n)
              quads3 (generate-quads n)]

          ;; first, insert 50 triples
          (let [stmt (jena/->update-string [(jena/insert-data-stmt (concat quads1 quads2))])]
            (apply-update handler test-editor draftset-location stmt))

          ;; then, delete 25 and insert 25 different ones
          (let [stmt (jena/->update-string [(jena/delete-data-stmt quads1)
                                            (jena/insert-data-stmt quads3)])]
            (apply-update handler test-editor draftset-location stmt))
          (let [draftset-quads (help/get-draftset-quads repo draftset-location)
                expected (set (concat quads2 quads3))]
            (t/is (= expected draftset-quads)))))

      (testing "DELETE/INSERT too large payload"
        (let [draftset-location (help/create-draftset-through-api handler test-editor)
              n 26

              ;; NOTE: The overall size of the update is calculated base on the total number of graphs affected so
              ;; the following must have a different graph for each quad. The default quad generator generates
              ;; graphs using the UUID generator
              quads1 (gen/sample (quad-gen) n)
              quads2 (gen/sample (quad-gen) n)
              quads3 (gen/sample (quad-gen) n)

              ;; INSERT up to the max update size should be accepted
              initial-insert (take max-update-size (shuffle (concat quads1 quads2)))
              stmt (jena/->update-string [(jena/insert-data-stmt initial-insert)])
              _ (apply-update handler test-editor draftset-location stmt)

              ;; combined INSERT/DELETE query should be rejected since total number of graphs exceeds the limit
              ;; each individual sub-query is below the limit
              stmt (jena/->update-string [(jena/delete-data-stmt quads1)
                                          (jena/insert-data-stmt quads3)])
              response (submit-update handler test-editor draftset-location stmt)]
          (tc/assert-is-payload-too-large-response response)

          ;; initial insert data should be in draft since last update was rejected
          (let [expected (set initial-insert)
                draftset-quads (help/get-draftset-quads repo draftset-location)]
            (t/is (= expected draftset-quads))))))))

(t/deftest delete-from-draft-test
  (tc/with-system
    keys-for-test
    [system system-config]
    (let [repo (:drafter/backend system)
          handler (get system [:drafter/routes :draftset/api])]
      (t/testing "Non-existent graph"
        (let [g (random-graph-uri)
              draftset-location (help/create-draftset-through-api handler test-publisher)
              to-delete (generate-graph-triples g 20)
              delete-query (jena/->update-string [(jena/delete-data-stmt to-delete)])]
          (apply-update handler test-publisher draftset-location delete-query)

          (t/is (= #{} (help/get-draftset-graph-triples repo draftset-location g)))

          ;; managed graph should be created within the draft
          (t/is (= :draft (get-graph-state repo g)))))

      (t/testing "Draft-only graph"
        (let [g (random-graph-uri)
              draftset-location (help/create-draftset-through-api handler test-publisher)
              to-insert (generate-graph-triples g 20)
              to-delete (concat (drop 30 to-insert) (generate-graph-triples g 10))
              insert-query (jena/->update-string [(jena/insert-data-stmt to-insert)])
              delete-query (jena/->update-string [(jena/delete-data-stmt to-delete)])]
          ;; insert quads within draft then submit delete
          (apply-update handler test-publisher draftset-location insert-query)
          (apply-update handler test-publisher draftset-location delete-query)

          (let [expected-quads (set/difference (set to-insert) (set to-delete))
                expected-triples (set (map pr/map->Triple expected-quads))
                triples (help/get-draftset-graph-triples repo draftset-location g)]
            (t/is (= expected-triples triples)))

          ;; graph should be managed and private
          (t/is (= :draft (get-graph-state repo g))))))))

(t/deftest delete-from-live-graph-test
  (tc/with-system
    keys-for-test
    [system system-config]
    (let [repo (:drafter/backend system)
          handler (get system [:drafter/routes :draftset/api])
          graph-uri (random-graph-uri)
          live-quads (generate-graph-triples graph-uri 20)]
      (help/publish-quads-through-api handler live-quads)

      (let [draftset-location (help/create-draftset-through-api handler test-publisher)
            to-delete (take 3 live-quads)
            delete-query (jena/->update-string [(jena/delete-data-stmt to-delete)])]
        (apply-update handler test-publisher draftset-location delete-query)

        (let [draft-graph-triples (help/get-draftset-graph-triples repo draftset-location graph-uri)
              expected (set (map pr/map->Triple (set/difference (set live-quads) (set to-delete))))]
          (t/is (= expected draft-graph-triples))

          ;;graph should still be live
          (t/is (= :live (get-graph-state repo graph-uri))))))))

(t/deftest delete-from-large-live-graph-test
  (tc/with-system
    keys-for-test [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])
          repo (:drafter/backend system)
          g (random-graph-uri)]
      (testing "Copies graph, deletes triple from it"
        ;; create largest allowable live graph that can be cloned
        (let [live-quads (generate-graph-triples g max-update-size)]
          (help/publish-quads-through-api handler live-quads)

          (let [draftset-location (help/create-draftset-through-api handler test-publisher)
                to-delete (take 13 live-quads)
                stmt (jena/->update-string [(jena/delete-data-stmt to-delete)])]
            (apply-update handler test-publisher draftset-location stmt)
            (let [draftset-quads (help/get-draftset-quads repo draftset-location)
                  expected (set/difference (set live-quads) (set to-delete))]
              (t/is (= expected draftset-quads))))

          ;; graph should still be live
          (t/is (= :live (get-graph-state repo g)))))

      (testing "Fail on trying to copy large graphs"
        ;; There should already contain the maximum number of allowed triples
        ;; Add more so live graph is now too large to clone
        (let [new-live-quads (generate-graph-triples g (* 2 max-update-size))]
          (help/publish-quads-through-api handler new-live-quads)

          (let [live-triples (help/get-live-graph-triples repo g)]
            (t/is (= (* 3 max-update-size) (count live-triples))))

          ;; attempting to delete from the live graph should fail since it is too large to clone
          (let [draftset-location (help/create-draftset-through-api handler test-publisher)
                to-delete (take 1 new-live-quads)
                stmt (jena/->update-string [(jena/insert-data-stmt to-delete)])
                response (submit-update handler test-publisher draftset-location stmt)]
            (tc/assert-is-server-error response)
            (t/is (= #{} (help/get-draftset-quads repo draftset-location))))

          ;; graph should still be live
          (t/is (= :live (get-graph-state repo g))))))))

(t/deftest drop-graph-visible-only-in-other-draftset-test
  (tc/with-system
    keys-for-test
    [system system-config]
    (let [repo (:drafter/backend system)
          handler  (get system [:drafter/routes :draftset/api])
          graph-uri (random-graph-uri)]
      ;; create a graph within a draft
      ;; create another draft and drop the same graph
      ;; graph does not exist from the perspective of the new draft
      ;;  => DROP GRAPH should error
      ;;  => DROP SILENT GRAPH should be a no-op and not create a draft graph
      (let [other-draftset-location (help/create-draftset-through-api handler test-editor)
            triples (generate-graph-triples graph-uri 20)
            insert-query (jena/->update-string [(jena/insert-data-stmt triples)])]
        (apply-update handler test-editor other-draftset-location insert-query))

      (let [draftset-location (help/create-draftset-through-api handler test-publisher)
            draftset-ref (help/location->draftset-ref draftset-location)]
        (t/testing "DROP GRAPH"
          (let [drop-query (str "DROP GRAPH <" graph-uri ">")
                response (submit-update handler test-publisher draftset-location drop-query)]
            (tc/assert-is-server-error response)
            (t/is (= false (mgmt-helpers/draft-exists? repo graph-uri draftset-ref)))))

        (t/testing "DROP SILENT GRAPH"
          (let [drop-query (str "DROP SILENT GRAPH <" graph-uri ">")]
            (apply-update handler test-publisher draftset-location drop-query)
            (t/is (= false (mgmt-helpers/draft-exists? repo graph-uri draftset-ref)))))))))

(t/deftest drop-graph-exists-only-in-draft-test
  (tc/with-system
    keys-for-test
    [system system-config]
    (let [repo (:drafter/backend system)
          handler (get system [:drafter/routes :draftset/api])
          graph-uri (random-graph-uri)
          quads (generate-graph-triples graph-uri 20)
          draftset-location (help/create-draftset-through-api handler test-publisher)]
      (apply-update handler test-publisher draftset-location (jena/->update-string [(jena/insert-data-stmt quads)]))

      ;;drop graph
      (let [drop-query (str "DROP GRAPH <" graph-uri ">")]
        (apply-update handler test-publisher draftset-location drop-query))

      (t/is (= 0 (count (help/get-draftset-graph-triples repo draftset-location graph-uri))))

      (t/is (= :draft (get-graph-state repo graph-uri))))))

(t/deftest drop-graph-test
  (tc/with-system
    keys-for-test [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])
          repo (:drafter/backend system)]

      (testing "Add quads, drop graph"
        (let [g (random-graph-uri)
              draftset-location (help/create-draftset-through-api handler test-publisher)
              n 50
              quads1 (generate-graph-triples g n)
              stmt (jena/->update-string [(jena/insert-data-stmt quads1)])]
          (apply-update handler test-publisher draftset-location stmt)
          (apply-update handler test-publisher draftset-location (format "DROP GRAPH <%s>" g))
          (t/is (= #{} (help/get-draftset-graph-triples repo draftset-location g)))

          ;; draft graph should still exist
          (t/is (= :draft (get-graph-state repo g)))))

      (testing "Add quads, drop graph, fail too big"
        (let [g (random-graph-uri)
              draftset-location (help/create-draftset-through-api handler test-publisher)
              quads (generate-graph-triples g (* 2 max-update-size))
              _ (help/append-quads-to-draftset-through-api handler test-publisher draftset-location quads)

              stmt (format "DROP GRAPH <%s>" g)
              response (submit-update handler test-publisher draftset-location stmt)
              draft-graph-triples (help/get-draftset-graph-triples repo draftset-location g)
              expected (set (map pr/map->Triple quads))]
          (tc/assert-is-server-error response)
          (t/is (= expected draft-graph-triples))

          ;; graph should still exist within the draft
          (t/is (= :draft (get-graph-state repo g)))))

      (testing "Add quads, publish, drop graph"
        (let [g (random-graph-uri)
              n 50
              quads1 (generate-graph-triples g n)
              _ (help/publish-quads-through-api handler quads1)

              draftset-location (help/create-draftset-through-api handler test-publisher)]

          ;; drop draft graph
          (apply-update handler test-publisher draftset-location (format "DROP GRAPH <%s>" g))

          ;; graph should exist within draft
          (let [draftset-ref (help/location->draftset-ref draftset-location)]
            (t/is (= true (mgmt-helpers/draft-exists? repo g draftset-ref))))

          ;; draft graph should be empty
          (let [draft-graph-triples (help/get-draftset-graph-triples repo draftset-location g)]
            (t/is (= #{} draft-graph-triples)))

          ;; data should still exist in live
          (let [live-graph-triples (help/get-live-graph-triples repo g)
                expected (set (map pr/map->Triple quads1))]
            (t/is (= expected live-graph-triples)))

          ;; graph should still be live
          (t/is (= :live (get-graph-state repo g)))

          ;; publish empty graph
          (help/publish-draftset-through-api handler draftset-location test-publisher)

          (t/is (= #{} (help/get-live-graph-triples repo g)))

          ;; graph should be deleted
          (t/is (= :unmanaged (get-graph-state repo g)))))

      (testing "Add quads and drop graph in one statement - noop"
        (let [g (random-graph-uri)
              draftset-location (help/create-draftset-through-api handler test-publisher)
              n 49
              quads1 (generate-graph-triples g n)
              stmt (jena/->update-string [(jena/insert-data-stmt quads1)
                                          (format "DROP GRAPH <%s>" g)])]
          (apply-update handler test-publisher draftset-location stmt)

          (t/is (= #{} (help/get-draftset-graph-triples repo draftset-location g)))

          ;; graph should be created within the draft
          (t/is (= :draft (get-graph-state repo g)))))

      (testing "DROP SILENT GRAPH then add quads in one statement - just adds quads"
        (let [g (random-graph-uri)
              draftset-location (help/create-draftset-through-api handler test-publisher)
              n 49
              quads1 (generate-graph-triples g n)

              stmt (jena/->update-string [(format "DROP SILENT GRAPH <%s>" g)
                                          (jena/insert-data-stmt quads1)])]
          (apply-update handler test-publisher draftset-location stmt)

          (let [draftset-graph-triples (help/get-draftset-graph-triples repo draftset-location g)
                expected (set (map pr/map->Triple quads1))]
            (t/is (= expected draftset-graph-triples)))

          ;; graph should be created within the draft
          (t/is (= :draft (get-graph-state repo g)))))

      (testing "DROP GRAPH then add quads in one statement - errors with live graph message"
        (let [g (random-graph-uri)
              draftset-location (help/create-draftset-through-api handler test-publisher)
              n 49
              quads1 (generate-graph-triples g n)
              stmt (jena/->update-string [(format "DROP GRAPH <%s>" g)
                                          (jena/insert-data-stmt quads1)])
              response (submit-update handler test-publisher draftset-location stmt)]
          (tc/assert-is-server-error response)
          (is (.contains (:message (:body response)) (str g)))

          ;; managed graph should not be created
          (t/is (= false (mgmt/is-graph-managed? repo g)))

          (t/is (= :unmanaged (get-graph-state repo g)))))

      (testing "DROP SILENT non-existent GRAPH - noop"
        (let [g (random-graph-uri)
              draftset-location (help/create-draftset-through-api handler test-publisher)
              stmt (format "DROP SILENT GRAPH <%s>" g)]
          (apply-update handler test-publisher draftset-location stmt)

          ;; managed graph should not be created
          (t/is (= false (mgmt/is-graph-managed? repo g)))

          ;; graph should not be managed
          (t/is (= :unmanaged (get-graph-state repo g)))))

      (testing "DROP non-existent GRAPH - Error"
        (let [g (URI. (str "http://g/" (UUID/randomUUID)))
              draftset-location (help/create-draftset-through-api handler test-publisher)
              stmt (format "DROP GRAPH <%s>" g)
              response (submit-update handler test-publisher draftset-location stmt)]
          (tc/assert-is-server-error response)

          ;; managed graph should not be created
          (t/is (= false (mgmt/is-graph-managed? repo g)))
          (t/is (= :unmanaged (get-graph-state repo g)))))

      (testing "DROP GRAPH g; from live, drops graph from live"
        (let [g (random-graph-uri)
              n 50
              quads1 (generate-graph-triples g n)]

          (help/publish-quads-through-api handler quads1)

          ;; DROP graph within draft and publish
          (let [draftset-location (help/create-draftset-through-api handler test-publisher)
                stmt (format "DROP GRAPH <%s>" g)]
            (apply-update handler test-publisher draftset-location stmt)
            (help/publish-draftset-through-api handler draftset-location test-publisher))

          (t/is (= #{} (help/get-live-graph-triples repo g)))
          (t/is (= :unmanaged (get-graph-state repo g)))))

      (testing "DROP GRAPH g; INSERT DATA { GRAPH g { ... } } - only new triples left in live"
        (let [g (random-graph-uri)
              n 50
              initial-live-quads (generate-graph-triples g n)
              new-draft-quads (generate-graph-triples g 2)]
          (help/publish-quads-through-api handler initial-live-quads)

          (let [draftset-location (help/create-draftset-through-api handler test-publisher)
                stmt (jena/->update-string [(format "DROP GRAPH <%s>" g)
                                            (jena/insert-data-stmt new-draft-quads)])]
            (apply-update handler test-publisher draftset-location stmt)
            (help/publish-draftset-through-api handler draftset-location test-publisher))

          (let [live-graph-triples (help/get-live-graph-triples repo g)
                expected (set (map pr/map->Triple new-draft-quads))]
            (t/is (= expected live-graph-triples)))

          (t/is (= :live (get-graph-state repo g))))))))

(defn metadata-q [draftset-uri]
  (format "
SELECT * WHERE {
  GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
    ?lg a <http://publishmydata.com/def/drafter/ManagedGraph> .
    ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://publishmydata.com/def/drafter/DraftSet> .
    ?dg <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
    ?lg <http://publishmydata.com/def/drafter/hasDraft> ?dg .
    ?dg <http://purl.org/dc/terms/created> ?dg_created .
    ?dg <http://purl.org/dc/terms/modified> ?dg_modified .
    ?ds <http://purl.org/dc/terms/created> ?ds_created .
    ?ds <http://purl.org/dc/terms/modified> ?ds_modified .
    ?ds <http://publishmydata.com/def/drafter/version> ?ds_version .
  }
  VALUES ?ds { <%s> }
}" draftset-uri))

(t/deftest draft-graph-metadata-test
  (tc/with-system
    keys-for-test [system system-config]
    (with-open [conn (-> system
                         :drafter.common.config/sparql-query-endpoint
                         repo/sparql-repo
                         repo/->connection)]
      (let [handler (get system [:drafter/routes :draftset/api])]

        (testing "Live graph g; DELETE DATA from g; check metadata"
          (let [g (random-graph-uri)
                n 50
                quads1 (generate-graph-triples g n)
                _ (help/publish-quads-through-api handler quads1)

                draftset-location (help/create-draftset-through-api handler test-publisher)
                draftset-id (help/location->draftset-ref draftset-location)
                draftset-uri (ds/->draftset-uri draftset-id)

                to-delete (take 5 quads1)
                stmt (jena/->update-string [(jena/delete-data-stmt to-delete)])
                _ (apply-update handler test-publisher draftset-location stmt)

                [{:keys [dg_created dg_modified
                         ds_created ds_modified] :as ds-meta-1}]
                (repo/query conn (metadata-q draftset-uri))
                _ (is (= ds_modified ds_created))
                _ (is (= dg_modified dg_created))

                ;; 2nd go, now the graph will already be copied, so modified and
                ;; version should update.
                stmt (jena/->update-string [(jena/delete-data-stmt to-delete)])
                _ (apply-update handler test-publisher draftset-location stmt)

                [{:keys [dg_created dg_modified
                         ds_created ds_modified] :as ds-meta-2}]
                (repo/query conn (metadata-q draftset-uri))]
            (is (.isAfter ds_modified ds_created))
            (is (.isAfter dg_modified dg_created))
            (is (not= (:ds_version ds-meta-1) (:ds_version ds-meta-2)))
            (is (= :live (get-graph-state conn g)))))

      (testing "Live graph g; INSERT DATA into g; check metadata"
        ;; Almost identical code path to DELETE, but here for completeness
        (let [g (random-graph-uri)
              n 50
              quads1 (generate-graph-triples g n)
              _ (help/publish-quads-through-api handler quads1)

              draftset-location (help/create-draftset-through-api handler test-publisher)
              draftset-id (help/location->draftset-ref draftset-location)
              draftset-uri (ds/->draftset-uri draftset-id)

              quads2 (generate-graph-triples g n)
              stmt (jena/->update-string [(jena/insert-data-stmt quads2)])
              _ (apply-update handler test-publisher draftset-location stmt)

              [{:keys [dg_created dg_modified
                       ds_created ds_modified] :as ds-meta-1}]
              (repo/query conn (metadata-q draftset-uri))
              _ (is (= ds_modified ds_created))
              _ (is (= dg_modified dg_created))

              ;; 2nd go, now the graph will already be copied, so modified
              ;; should update.
              quads3 (generate-graph-triples g n)
              stmt (jena/->update-string [(jena/insert-data-stmt quads3)])
              _ (apply-update handler test-publisher draftset-location stmt)

              [{:keys [dg_created dg_modified
                       ds_created ds_modified] :as ds-meta-2}]
              (repo/query conn (metadata-q draftset-uri))]
          (is (.isAfter ds_modified ds_created))
          (is (.isAfter dg_modified dg_created))
          (is (not= (:ds_version ds-meta-1) (:ds_version ds-meta-2)))
          (is (= :live (get-graph-state conn g)))))

      (testing "Live graph g, with draft graph; DROP GRAPH g; check metadata"
        (let [g (random-graph-uri)
              n 10
              quads1 (generate-graph-triples g n)
              _ (help/publish-quads-through-api handler quads1)

              ;; INSERT DATA so that graph g makes it into the draftset
              draftset-location (help/create-draftset-through-api handler test-publisher)
              draftset-id (help/location->draftset-ref draftset-location)
              draftset-uri (ds/->draftset-uri draftset-id)

              quads2 (generate-graph-triples g n)
              stmt (jena/->update-string [(jena/insert-data-stmt quads2)])
              _ (apply-update handler test-publisher draftset-location stmt)

              [{:keys [dg_created dg_modified
                       ds_created ds_modified] :as ds-meta-1}]
              (repo/query conn (metadata-q draftset-uri))
              _ (is (= ds_modified ds_created))
              _ (is (= dg_modified dg_created))

              ;; The graph will now be copied, so test DROP GRAPH
              stmt (format "DROP GRAPH <%s>" g)
              _ (apply-update handler test-publisher draftset-location stmt)

              [{:keys [dg_created dg_modified
                       ds_created ds_modified] :as ds-meta-2}]
              (repo/query conn (metadata-q draftset-uri))]
          (is (.isAfter ds_modified ds_created))
          (is (.isAfter dg_modified dg_created))
          (is (= :live (get-graph-state conn g)))
          (is (not= (:ds_version ds-meta-1) (:ds_version ds-meta-2)))))))))

(t/deftest protected-graphs-test
  (tc/with-system
    keys-for-test [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])
          repo (:drafter/backend system)]
      (testing "Cannot operate on drafter's graphs"
        (let [g (URI. "http://publishmydata.com/graphs/drafter/drafts")
              draftset-location (help/create-draftset-through-api handler test-editor)
              quad (pr/->Quad (URI. "http://s") (URI. "http://p") (URI. "http://o") g)]

          (testing "INSERT DATA ..."
            (let [update-request (jena/->update-string [(jena/insert-data-stmt [quad])])
                  response (submit-update handler test-editor draftset-location update-request)]
              (tc/assert-is-forbidden-response response)
              (t/is (= :unmanaged (get-graph-state repo g)))))

          (testing "DELETE DATA ..."
            (let [update-request (jena/->update-string [(jena/delete-data-stmt [quad])])
                  response (submit-update handler test-editor draftset-location update-request)]
              (tc/assert-is-forbidden-response response)
              (t/is (= :unmanaged (get-graph-state repo g)))))

          (testing "DROP GRAPH ..."
            (let [update-request (format "DROP GRAPH <%s>" g)
                  response (submit-update handler test-editor draftset-location update-request)]
              (tc/assert-is-forbidden-response response)
              (t/is (= :unmanaged (get-graph-state repo g))))))))))

(t/deftest access-forbidden-graphs-test
  (tc/with-system
    keys-for-test [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])
          backend (:drafter/backend system)]

      (testing "Cannot see draft graphs in other draftsets"
        ;; I.E., a user can operate on the same live graph in another draftset
        ;; but cannot access the draft graph of another draftset
        (let [g (random-graph-uri)
              draftset-location (help/create-draftset-through-api handler test-publisher)
              draftset-id (help/location->draftset-ref draftset-location)
              quad (pr/->Quad (URI. "http://s") (URI. "http://p") (URI. "http://o") g)
              _ (help/append-quads-to-draftset-through-api handler test-publisher draftset-location [quad])

              dg (dsops/find-draftset-draft-graph backend draftset-id g)
              _ (t/is (some? dg) "Draft graph not found")

              draftset-location (help/create-draftset-through-api handler test-editor)
              draftset-id (last (string/split draftset-location #"/"))
              quad (pr/->Quad (URI. "http://s") (URI. "http://p") (URI. "http://o") g)
              update-request (jena/->update [(jena/insert-data-stmt [quad])])
              gmeta (#'update/get-graph-meta backend
                                             draftset-id
                                             update-request
                                             50)
              draft-graph-uri (get-in gmeta [g :draft-graph-uri])]
          (is (not= dg draft-graph-uri))
          (is (not (nil? draft-graph-uri))))))))


(t/deftest interleaved-updates-and-queries
  (tc/with-system
    keys-for-test [system manual-clock-config]
    (let [clock (:drafter.test-common/manual-clock system)
          handler (get system [:drafter/routes :draftset/api])
          draftset-location (help/create-draftset-through-api handler test-editor)
          g (random-graph-uri)
          insert-quads (fn [quads]
                        (apply-update handler
                                      test-editor
                                      draftset-location
                                      (jena/->update-string
                                       [(jena/insert-data-stmt quads)])))
          count-query (fn []
                        (handler
                         (create-query-request
                          test-editor
                          draftset-location
                          (format "SELECT (COUNT(*) AS ?count) WHERE {
                                   GRAPH <%s> { ?s ?p ?o }
                                   }"
                                  g)
                          "text/csv")))]
      ;; Stop the clock, so all three of the following take place "within a
      ;; millisecond".
      (tc/stop clock)
      (insert-quads (generate-graph-triples g 1))
      (tc/assert-is-ok-response (count-query))
      (insert-quads (generate-graph-triples g 1))
      (tc/resume clock)
      ;; If the cache only uses millisecond precision, the preceeding count-query
      ;; will have been cached and the following will fail.
      (let [res (count-query)
            count (-> res :body slurp string/split-lines second read-string)]
        (tc/assert-is-ok-response res)
        (is (= 2 count))))))
