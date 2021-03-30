(ns ^:rest-api drafter.feature.draftset.update-test
  (:require
   [drafter.feature.middleware :as middleware]
   [clojure.string :as string]
   [clojure.test :as t :refer [is testing]]
   [drafter.draftset :as ds]
   [drafter.feature.draftset.query-test :refer [create-query-request]]
   [drafter.feature.draftset.test-helper :as help]
   [drafter.feature.draftset.update :as update]
   [drafter.rdf.jena :as jena]
   [drafter.test-common :as tc]
   [drafter.user-test :refer [test-editor test-publisher]]
   [grafter-2.rdf.protocols :as pr]
   [grafter-2.rdf4j.repository :as repo])
  (:import
   java.net.URI
   java.util.UUID))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "drafter/feature/empty-db-system.edn")

(def keys-for-test
  [[:drafter/routes :draftset/api]
   :drafter/backend
   :drafter/write-scheduler
   :drafter.routes.sparql/live-sparql-query-route
   :drafter.backend.live/endpoint
   :drafter.common.config/sparql-query-endpoint])

(defn- create-update-request
  [user draftset-location stmt]
  (tc/with-identity user
    {:uri (str draftset-location "/update")
     :headers {"accept" "text/plain"
               "content-type" "application/sparql-update"}
     :request-method :post
     :body stmt}))

(defn draftset-quads-mapping-q [draftset-location]
  (let [draftset-id (last (string/split draftset-location #"/"))
        draftset-uri (ds/->draftset-uri draftset-id)
        q "
SELECT ?lg ?dg ?s ?p ?o WHERE {
  GRAPH ?dg { ?s ?p ?o }
  GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
    BIND ( <%s> AS ?ds )
    ?ds <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
      <http://publishmydata.com/def/drafter/DraftSet> .
    ?dg <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
    ?lg <http://publishmydata.com/def/drafter/hasDraft> ?dg .
  }
}"]
    (format q draftset-uri)))

(tc/deftest-system-with-keys insert-modify-test
  keys-for-test [system system-config]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (let [handler (get system [:drafter/routes :draftset/api])
          draftset-location (help/create-draftset-through-api handler test-editor)
          update! (fn [stmt]
                    (handler (create-update-request
                              test-editor draftset-location stmt)))
          g (URI. (str "http://g/" (UUID/randomUUID)))
          stmt (format " INSERT DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } " g)
          response (update! stmt)
          _ (tc/assert-is-no-content-response response)
          stmt (format "
PREFIX test: <http://test/>
DELETE DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } ;
INSERT DATA { GRAPH <%s> { <http://s> <http://p> <%s> } }
" g g g)
          response (update! stmt)
          _ (tc/assert-is-no-content-response response)
          [{:keys [dg s p o]} :as quads]
          (repo/query conn (draftset-quads-mapping-q draftset-location))]
      (is (not (nil? dg)))
      (is (= 1 (count quads)))
      (is (= dg o))
      (is (not= o (URI. "http://g"))))))

(tc/deftest-system-with-keys prefix-mapping-rewrite-test
  ;; Same test as above, but use a prefix mapping - actually works for free
  keys-for-test [system system-config]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (let [handler (get system [:drafter/routes :draftset/api])
          draftset-location (help/create-draftset-through-api handler test-editor)
          update! (fn [stmt]
                    (handler (create-update-request
                              test-editor draftset-location stmt)))
          g (URI. (str "http://g/" (UUID/randomUUID)))
          stmt (format " INSERT DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } " g)
          response (update! stmt)
          _ (tc/assert-is-no-content-response response)
          stmt (format "
PREFIX d: <%s>
DELETE DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } ;
INSERT DATA { GRAPH <%s> { <http://s> <http://p> d: } }
" g g g)
          response (update! stmt)
          _ (tc/assert-is-no-content-response response)
          [{:keys [dg s p o]} :as quads]
          (repo/query conn (draftset-quads-mapping-q draftset-location))]
      (is (not (nil? dg)))
      (is (= 1 (count quads)))
      (is (= dg o))
      (is (not= o (URI. "http://g"))))))

(tc/deftest-system-with-keys base-uri-rewrite-test
  ;; Same test as above, but use a prefix mapping - actually works for free
  keys-for-test [system system-config]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (let [handler (get system [:drafter/routes :draftset/api])
          draftset-location (help/create-draftset-through-api handler test-editor)
          update! (fn [stmt]
                    (handler (create-update-request
                              test-editor draftset-location stmt)))
          uuid (UUID/randomUUID)
          g (URI. (str "http://g/" uuid))
          stmt (format " INSERT DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } " g)
          response (update! stmt)
          _ (tc/assert-is-no-content-response response)
          stmt (format "
BASE <http://g/>
PREFIX d: <%s>
DELETE DATA { GRAPH <%s> { <http://s> <http://p> <http://o> } } ;
INSERT DATA { GRAPH <%s> { <http://s> <http://p> d: } }
" uuid uuid uuid)
          response (update! stmt)
          _ (tc/assert-is-no-content-response response)
          [{:keys [dg s p o]} :as quads]
          (repo/query conn (draftset-quads-mapping-q draftset-location))]
      (is (not (nil? dg)))
      (is (= 1 (count quads)))
      (is (= dg o))
      (is (not= o (URI. "http://g"))))))

(def valid-triples
  (->> (range)
       (map (fn [i]
              [(URI. (str "http://g/" i))
               (URI. (str "http://s/" i))
               (URI. (str "http://p/" i))
               (URI. (str "http://g/" i))]))))

(defn valid-triples-g [g]
  (->> (range)
       (map (fn [i]
              [g (URI. (str "http://s/" i)) (URI. (str "http://p/" i)) g]))))

(defn quad-pattern-str [quad]
  (apply format "GRAPH <%s> { <%s> <%s> <%s> }" quad))

(defn insert-stmt-str [quads]
  (str "INSERT DATA { "
       (string/join " . \n" (map quad-pattern-str quads))
       "}"))

(defn delete-stmt-str [quads]
  (str "DELETE DATA { "
       (string/join " . \n" (map quad-pattern-str quads))
       "}"))

(defn rewrite [quads mapping]
  (letfn [(rewrite1 [x] (get mapping x x))]
    (map (fn [q]
           (-> q
               (update 0 rewrite1)
               (update 1 rewrite1)
               (update 2 rewrite1)
               (update 3 rewrite1)))
         quads)))

(tc/deftest-system-with-keys insert-and-delete-max-payload-test
  keys-for-test [system system-config]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (let [handler (get system [:drafter/routes :draftset/api])]
      (testing "Maximum size of payload"
        (testing "Insert max payload"
          (let [draftset-location (help/create-draftset-through-api handler test-editor)
                update! (fn [stmt]
                          (handler (create-update-request
                                    test-editor draftset-location stmt)))
                n 50
                quads (take n valid-triples)
                stmt (insert-stmt-str quads)
                response (update! stmt)
                res (repo/query conn (draftset-quads-mapping-q draftset-location))
                mapping (into {} (map (juxt :lg :dg) res))

                quads' (set (map (juxt :dg :s :p :o) res))]
            (tc/assert-is-no-content-response response)
            (is (= n (count quads')))
            (is (= (set (rewrite quads mapping)) quads'))

            (testing "Delete max payload"
              (tc/assert-is-no-content-response (update! (delete-stmt-str quads)))
              (is (zero? (count (repo/query conn (draftset-quads-mapping-q draftset-location)))))))))

      (testing "Too large payload"
        (testing "Insert too large payload"
          (let [draftset-location (help/create-draftset-through-api handler test-editor)
                update! (fn [stmt]
                          (handler (create-update-request
                                    test-editor draftset-location stmt)))
                n 51
                quads (take n valid-triples)
                stmt (insert-stmt-str quads)
                response (update! stmt)
                res (repo/query conn (draftset-quads-mapping-q draftset-location))]
            (tc/assert-is-payload-too-large-response response)
            (is (zero? (count res)))
            (testing "Delete too large payload"
              (tc/assert-is-payload-too-large-response (update! (delete-stmt-str quads))))))))))

(tc/deftest-system-with-keys DELETE_INSERT-max-payload-test
  keys-for-test [system system-config]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (let [handler (get system [:drafter/routes :draftset/api])]
      (testing "DELETE/INSERT max payload"
        (let [draftset-location (help/create-draftset-through-api handler test-editor)
              update! (fn [stmt]
                        (handler (create-update-request
                                  test-editor draftset-location stmt)))
              n 25
              [quads1 more] (split-at n valid-triples)
              [quads2 more] (split-at n more)
              [quads3 more] (split-at n more)

              ;; first, insert 50 triples
              stmt (insert-stmt-str (concat quads1 quads2))
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)

              ;; then, delete 25 and insert 25 different ones
              stmt (str (delete-stmt-str quads1) \;
                        (insert-stmt-str quads3))
              response (update! stmt)
              res (repo/query conn (draftset-quads-mapping-q draftset-location))
              mapping (into {} (map (juxt :lg :dg) res))
              quads  (set (rewrite (concat quads2 quads3) mapping))
              quads' (set (map (juxt :dg :s :p :o) res))]
          (tc/assert-is-no-content-response response)
          (is (= (* 2 n) (count quads')))
          (is (= quads quads'))))

      (testing "DELETE/INSERT too large payload"
        (let [draftset-location (help/create-draftset-through-api handler test-editor)
              update! (fn [stmt]
                        (handler (create-update-request
                                  test-editor draftset-location stmt)))
              n 26
              [quads1 more] (split-at n valid-triples)
              [quads2 more] (split-at n more)
              [quads3 more] (split-at n more)

              stmt (insert-stmt-str (drop 2 (concat quads1 quads2)))
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)

              stmt (str (delete-stmt-str quads1) \;
                        (insert-stmt-str quads3))
              response (update! stmt)
              res (repo/query conn (draftset-quads-mapping-q draftset-location))]
          (tc/assert-is-payload-too-large-response response)
          (is (= (- (* 2 n) 2) (count res))))))))

(t/deftest delete-from-live-test
  (tc/with-system
    keys-for-test [system system-config]
    (with-open [conn (-> system
                         :drafter.common.config/sparql-query-endpoint
                         repo/sparql-repo
                         repo/->connection)]
      (let [handler (get system [:drafter/routes :draftset/api])
            g (URI. (str "http://g/" (UUID/randomUUID)))]
        (testing "Copies graph, deletes triple from it"
          (let [draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                n 50
                [quads1 more] (split-at n (valid-triples-g g))
                stmt (insert-stmt-str quads1)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)
                _ (help/publish-draftset-through-api handler draftset-location test-publisher)
                ;; There should now be 50 triples live in graph g
                q "SELECT ?g ?s ?p ?o WHERE { BIND ( <%s> AS ?g ) GRAPH ?g { ?s ?p ?o } }"
                res (repo/query conn (format q g))
                _ (is (= 50 (count res)))

                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))

                stmt (delete-stmt-str [(first quads1)])
                response (update! stmt)
                res (repo/query conn (draftset-quads-mapping-q draftset-location))]
            (tc/assert-is-no-content-response response)
            (is (= 49 (count res)))))

        (testing "Fail on trying to copy large graphs"
          (let [;; There should already be 50 triples in live
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                n 50
                [quads1 more] (split-at n (drop 50 (valid-triples-g g)))
                [quads2 more] (split-at n more)
                stmt (insert-stmt-str quads1)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)
                stmt (insert-stmt-str quads2)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)
                _ (help/publish-draftset-through-api handler draftset-location test-publisher)
                q "SELECT ?g ?s ?p ?o WHERE { BIND ( <%s> AS ?g ) GRAPH ?g { ?s ?p ?o } }"
                res (repo/query conn (format q g))
                ;; There should now be 150 triples live in graph g
                _ (is (= 150 (count res)))

                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))

                stmt (delete-stmt-str [(first quads1)])
                response (update! stmt)
                res (repo/query conn (draftset-quads-mapping-q draftset-location))]
            (tc/assert-is-server-error response)
            (is (zero? (count res)))))))))

(t/deftest drop-graph-test
  (tc/with-system
    keys-for-test [system system-config]
    (with-open [conn (-> system
                         :drafter.common.config/sparql-query-endpoint
                         repo/sparql-repo
                         repo/->connection)]
      (let [handler (get system [:drafter/routes :draftset/api])]

        (testing "Add quads, drop graph"
          (let [g (URI. (str "http://g/" (UUID/randomUUID)))
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                n 50
                [quads1 more] (split-at n (valid-triples-g g))
                stmt (insert-stmt-str quads1)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)

                stmt (format "DROP GRAPH <%s>" g)
                response (update! stmt)
                res (repo/query conn (draftset-quads-mapping-q draftset-location))]
            (tc/assert-is-no-content-response response)
            (is (zero? (count res)))))

        (testing "Add quads, drop graph, fail too big"
          (let [g (URI. (str "http://g/" (UUID/randomUUID)))
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                n 50
                [quads1 more] (split-at n (valid-triples-g g))
                [quads2 more] (split-at n more)
                stmt (insert-stmt-str quads1)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)
                stmt (insert-stmt-str quads2)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)

                stmt (format "DROP GRAPH <%s>" g)
                response (update! stmt)
                res (repo/query conn (draftset-quads-mapping-q draftset-location))]
            (tc/assert-is-server-error response)
            (is (= 100 (count res)))))

        (testing "Add quads, publish, drop graph"
          (let [g (URI. (str "http://g/" (UUID/randomUUID)))
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                n 50
                [quads1 more] (split-at n (valid-triples-g g))
                stmt (insert-stmt-str quads1)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)
                _ (help/publish-draftset-through-api handler draftset-location test-publisher)
                q "SELECT ?g ?s ?p ?o WHERE { BIND ( <%s> AS ?g ) GRAPH ?g { ?s ?p ?o } }"
                res (repo/query conn (format q g))
                _ (is (= 50 (count res)))

                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                stmt (format "DROP GRAPH <%s>" g)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)
                res (repo/query conn (draftset-quads-mapping-q draftset-location))
                _ (is (zero? (count res)) "Ensure draft graph is empty")
                res (repo/query conn (format q g))
                _ (is (= 50 (count res)) "Ensure graph still in live")
                _ (help/publish-draftset-through-api handler draftset-location test-publisher)
                res (repo/query conn (format q g))]
            (tc/assert-is-no-content-response response)
            (is (zero? (count res)))))

        (testing "Add quads and drop graph in one statement - noop"
          (let [g (URI. (str "http://g/" (UUID/randomUUID)))
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                n 49
                [quads1 more] (split-at n (valid-triples-g g))
                stmt (str (insert-stmt-str quads1) ";\n"
                          (format "DROP GRAPH <%s>" g))
                response (update! stmt)
                res (repo/query conn (draftset-quads-mapping-q draftset-location))]
            (tc/assert-is-no-content-response response)
            (is (zero? (count res)))))

        (testing "DROP SILENT GRAPH then add quads in one statement - just adds quads"
          (let [g (URI. (str "http://g/" (UUID/randomUUID)))
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                n 49
                [quads1 more] (split-at n (valid-triples-g g))
                stmt (str (format "DROP SILENT GRAPH <%s>" g) ";\n"
                          (insert-stmt-str quads1))
                response (update! stmt)
                res (repo/query conn (draftset-quads-mapping-q draftset-location))]
            (tc/assert-is-no-content-response response)
            (is (= 49 (count res)))))

        (testing "DROP GRAPH then add quads in one statement - errors with live graph message"
          (let [g (URI. (str "http://g/" (UUID/randomUUID)))
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                n 49
                [quads1 more] (split-at n (valid-triples-g g))
                stmt (str (format "DROP GRAPH <%s>" g) ";\n"
                          (insert-stmt-str quads1))
                response (update! stmt)]
            (tc/assert-is-server-error response)
            (is (.contains (:message (:body response)) (str g)))))

        (testing "DROP SILENT non-existent GRAPH - noop"
          (let [g (URI. (str "http://g/" (UUID/randomUUID)))
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                stmt (format "DROP SILENT GRAPH <%s>" g)
                response (update! stmt)
                res (repo/query conn (draftset-quads-mapping-q draftset-location))]
            (tc/assert-is-no-content-response response)
            (is (zero? (count res)))))

        (testing "DROP non-existent GRAPH - Error"
          (let [g (URI. (str "http://g/" (UUID/randomUUID)))
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                stmt (format "DROP GRAPH <%s>" g)
                response (update! stmt)]
            (tc/assert-is-server-error response)))

        (testing "DROP GRAPH g; from live, drops graph from live"
          (let [g (URI. (str "http://g/" (UUID/randomUUID)))
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                n 50
                [quads1 more] (split-at n (valid-triples-g g))
                stmt (insert-stmt-str quads1)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)
                _ (help/publish-draftset-through-api handler draftset-location test-publisher)
                q "SELECT ?g ?s ?p ?o WHERE { BIND ( <%s> AS ?g ) GRAPH ?g { ?s ?p ?o } }"
                res (repo/query conn (format q g))
                _ (is (= 50 (count res)))

                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                stmt (format "DROP GRAPH <%s>" g)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)
                _ (help/publish-draftset-through-api handler draftset-location test-publisher)
                q "SELECT ?g ?s ?p ?o WHERE { BIND ( <%s> AS ?g ) GRAPH ?g { ?s ?p ?o } }"
                res (repo/query conn (format q g))
                q "SELECT * WHERE {
                   BIND ( <%s> AS ?g )
                   GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
                     ?g a <http://publishmydata.com/def/drafter/ManagedGraph> .
                   }
                 }"
                res2 (repo/query conn (format q g))]
            (is (zero? (count res)))
            (is (zero? (count res2)))))

        (testing "DROP GRAPH g; INSERT DATA { GRAPH g { ... } } - only new triples left in live"
          (let [g (URI. (str "http://g/" (UUID/randomUUID)))
                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                n 50
                [quads1 more] (split-at n (valid-triples-g g))
                stmt (insert-stmt-str quads1)
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)
                _ (help/publish-draftset-through-api handler draftset-location test-publisher)
                q "SELECT ?g ?s ?p ?o WHERE { BIND ( <%s> AS ?g ) GRAPH ?g { ?s ?p ?o } }"
                res (repo/query conn (format q g))
                _ (is (= 50 (count res)))

                draftset-location (help/create-draftset-through-api handler test-publisher)
                update! (fn [stmt]
                          (handler (create-update-request
                                     test-publisher draftset-location stmt)))
                [quads2 more] (split-at 2 more)
                stmt (str (format "DROP GRAPH <%s>" g) ";\n"
                          (insert-stmt-str quads2))
                response (update! stmt)
                _ (tc/assert-is-no-content-response response)
                _ (help/publish-draftset-through-api handler draftset-location test-publisher)
                q "SELECT ?g ?s ?p ?o WHERE { BIND ( <%s> AS ?g ) GRAPH ?g { ?s ?p ?o } }"
                res (repo/query conn (format q g))]
            (is (= 2 (count res)))
            (is (= quads2 (map (juxt :g :s :p :o) res)))))))))


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
  }
  VALUES ?ds { <%s> }
}" draftset-uri))

(tc/deftest-system-with-keys draft-graph-metadata-test
  keys-for-test [system system-config]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (let [handler (get system [:drafter/routes :draftset/api])]

      (testing "Live graph g; DELETE DATA from g; check metadata"
        (let [g (URI. (str "http://g/" (UUID/randomUUID)))
              draftset-location (help/create-draftset-through-api handler test-publisher)
              update! (fn [stmt]
                        (handler (create-update-request
                                  test-publisher draftset-location stmt)))
              n 50
              [quads1 more] (split-at n (valid-triples-g g))
              stmt (insert-stmt-str quads1)
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)
              _ (help/publish-draftset-through-api handler draftset-location test-publisher)
              q "SELECT ?g ?s ?p ?o WHERE { BIND ( <%s> AS ?g ) GRAPH ?g { ?s ?p ?o } }"
              res (repo/query conn (format q g))
              _ (is (= 50 (count res)))

              draftset-location (help/create-draftset-through-api handler test-publisher)
              draftset-id (last (string/split draftset-location #"/"))
              draftset-uri (ds/->draftset-uri draftset-id)
              update! (fn [stmt]
                        (handler (create-update-request
                                  test-publisher draftset-location stmt)))
              stmt (delete-stmt-str (take 5 quads1))
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)
              [{:keys [dg_created dg_modified
                       ds_created ds_modified] :as ds-meta}]
              (repo/query conn (metadata-q draftset-uri))
              _ (is (= ds_modified ds_created))
              _ (is (= dg_modified dg_created))

              ;; 2nd go, now the graph will already be copied, so modified
              ;; should update.
              stmt (delete-stmt-str (take 5 quads1))
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)
              [{:keys [dg_created dg_modified
                       ds_created ds_modified] :as ds-meta}]
              (repo/query conn (metadata-q draftset-uri))]
          (is (.isAfter ds_modified ds_created))
          (is (.isAfter dg_modified dg_created))))

      (testing "Live graph g; INSERT DATA into g; check metadata"
        ;; Almost identical code path to DELETE, but here for completeness
        (let [g (URI. (str "http://g/" (UUID/randomUUID)))
              draftset-location (help/create-draftset-through-api handler test-publisher)
              update! (fn [stmt]
                        (handler (create-update-request
                                  test-publisher draftset-location stmt)))
              n 50
              [quads1 more] (split-at n (valid-triples-g g))
              stmt (insert-stmt-str quads1)
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)
              _ (help/publish-draftset-through-api handler draftset-location test-publisher)
              q "SELECT ?g ?s ?p ?o WHERE { BIND ( <%s> AS ?g ) GRAPH ?g { ?s ?p ?o } }"
              res (repo/query conn (format q g))
              _ (is (= 50 (count res)))

              draftset-location (help/create-draftset-through-api handler test-publisher)
              draftset-id (last (string/split draftset-location #"/"))
              draftset-uri (ds/->draftset-uri draftset-id)
              update! (fn [stmt]
                        (handler (create-update-request
                                  test-publisher draftset-location stmt)))
              [quads2 more] (split-at 5 more)
              stmt (insert-stmt-str quads2)
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)
              [{:keys [dg_created dg_modified
                       ds_created ds_modified] :as ds-meta}]
              (repo/query conn (metadata-q draftset-uri))
              _ (is (= ds_modified ds_created))
              _ (is (= dg_modified dg_created))

              ;; 2nd go, now the graph will already be copied, so modified
              ;; should update.
              [quads3 more] (split-at 5 more)
              stmt (insert-stmt-str quads3)
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)
              [{:keys [dg_created dg_modified
                       ds_created ds_modified] :as ds-meta}]
              (repo/query conn (metadata-q draftset-uri))]
          (is (.isAfter ds_modified ds_created))
          (is (.isAfter dg_modified dg_created))))

      (testing "Live graph g, with draft graph; DROP GRAPH g; check metadata"
        (let [g (URI. (str "http://g/" (UUID/randomUUID)))
              draftset-location (help/create-draftset-through-api handler test-publisher)
              update! (fn [stmt]
                        (handler (create-update-request
                                  test-publisher draftset-location stmt)))
              n 10
              [quads1 more] (split-at n (valid-triples-g g))
              stmt (insert-stmt-str quads1)
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)
              _ (help/publish-draftset-through-api handler draftset-location test-publisher)
              q "SELECT ?g ?s ?p ?o WHERE { BIND ( <%s> AS ?g ) GRAPH ?g { ?s ?p ?o } }"
              res (repo/query conn (format q g))
              _ (is (= 10 (count res)))
              ;; INSERT DATA so that graph g makes it into the draftset
              draftset-location (help/create-draftset-through-api handler test-publisher)
              draftset-id (last (string/split draftset-location #"/"))
              draftset-uri (ds/->draftset-uri draftset-id)
              update! (fn [stmt]
                        (handler (create-update-request
                                  test-publisher draftset-location stmt)))
              [quads2 more] (split-at 5 more)
              stmt (insert-stmt-str quads2)
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)
              [{:keys [dg_created dg_modified
                       ds_created ds_modified] :as ds-meta}]
              (repo/query conn (metadata-q draftset-uri))
              _ (is (= ds_modified ds_created))
              _ (is (= dg_modified dg_created))
              ;; The graph will now be copied, so test DROP GRAPH
              stmt (format "DROP GRAPH <%s>" g)
              response (update! stmt)
              _ (tc/assert-is-no-content-response response)
              [{:keys [dg_created dg_modified
                       ds_created ds_modified] :as ds-meta}]
              (repo/query conn (metadata-q draftset-uri))]
          (is (.isAfter ds_modified ds_created))
          (is (.isAfter dg_modified dg_created)))))))

(t/deftest protected-graphs-test
  (tc/with-system
    keys-for-test [system system-config]
    (let [handler (get system [:drafter/routes :draftset/api])]
      (testing "Cannot operate on drafter's graphs"
        (let [g (URI. "http://publishmydata.com/graphs/drafter/drafts")
              draftset-location (help/create-draftset-through-api handler test-editor)
              update! (fn [stmt]
                        (->> stmt
                             (create-update-request test-editor draftset-location)
                             (handler)))
              quad (pr/->Quad (URI. "http://s") (URI. "http://p") (URI. "http://o") g)]

          (testing "INSERT DATA ..."
            (let [stmt (jena/insert-data-stmt [quad])
                  update-request (jena/->update-string [stmt])
                  response (update! (str update-request))]
              (tc/assert-is-forbidden-response response)))

          (testing "DELETE DATA ..."
            (let [stmt (jena/delete-data-stmt [quad])
                  update-request (jena/->update-string [stmt])
                  response (update! (str update-request))]
              (tc/assert-is-forbidden-response response)))

          (testing "DROP GRAPH ..."
            (let [stmt (format "DROP GRAPH <%s>" g)
                  update-request (jena/->update-string [stmt])
                  response (update! update-request)]
              (tc/assert-is-forbidden-response response))))))))

(tc/deftest-system-with-keys access-forbidden-graphs-test
  keys-for-test [system system-config]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (let [handler (get system [:drafter/routes :draftset/api])
          backend (:drafter/backend system)]

      (testing "Cannot see draft graphs in other draftsets"
        ;; I.E., a user can operate on the same live graph in another draftset
        ;; but cannot access the draft graph of another draftset
        (let [g (URI. (str "http://g/" (UUID/randomUUID)))
              draftset-location (help/create-draftset-through-api handler test-publisher)
              draftset-id (last (string/split draftset-location #"/"))
              draftset-uri (ds/->draftset-uri draftset-id)
              update! (fn [stmt]
                        (handler (create-update-request
                                  test-publisher draftset-location stmt)))
              quad (pr/->Quad (URI. "http://s") (URI. "http://p") (URI. "http://o") g)
              response (help/append-quads-to-draftset-through-api
                        handler test-publisher draftset-location [quad])
              q "SELECT ?dg WHERE {
                   VALUES ?ds { <%s> }
                   GRAPH <http://publishmydata.com/graphs/drafter/drafts> {
                     ?dg <http://publishmydata.com/def/drafter/inDraftSet> ?ds .
                   }
                 }"
              [{:keys [dg]}] (repo/query conn (format q draftset-uri))
              draftset-location (help/create-draftset-through-api handler test-editor)
              draftset-id (last (string/split draftset-location #"/"))
              quad (pr/->Quad (URI. "http://s") (URI. "http://p") (URI. "http://o") g)
              stmt (jena/insert-data-stmt [quad])
              update-request (jena/->update [stmt])
              gmeta (#'update/get-graph-meta backend draftset-id update-request)
              draft-graph-uri (get-in gmeta [g :draft-graph-uri])]
          (is (not= dg draft-graph-uri))
          (is (not (nil? draft-graph-uri))))))))

(tc/deftest-system-with-keys updates-in-quick-succession
  keys-for-test [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        draftset-location (help/create-draftset-through-api handler test-editor)
        update! (fn [stmt]
                  (handler (create-update-request
                            test-editor draftset-location stmt)))
        count-query (fn [g]
                      (handler
                       (create-query-request
                        test-editor
                        draftset-location
                        (format "SELECT (COUNT(*) AS ?count) WHERE {
                                 GRAPH <%s> { ?s ?p ?o }
                                 }"
                                g)
                        "text/csv")))]
    (dotimes [_ 10]
      (let [g (URI. (str "http://g/" (UUID/randomUUID)))]
        (doall
         (pmap
          #(%)
          [#(tc/assert-is-no-content-response
             (update!
              (format "INSERT DATA {
                       GRAPH <%s> { <http://s1> <http://p1> <http://o1> }
                       }"
                      g)))
           #(tc/assert-is-no-content-response
             (update!
              (format "INSERT DATA {
                       GRAPH <%s> { <http://s2> <http://p2> <http://o2> }
                       }"
                      g)))]))
        (let [res (count-query g)
              count (-> res :body slurp string/split-lines second read-string)]
          (tc/assert-is-ok-response res)
          (is (= 2 count)))))))
