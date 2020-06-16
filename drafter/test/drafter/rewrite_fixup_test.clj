(ns drafter.rewrite-fixup-test
  (:require [cheshire.core :as json]
            [clojure.test :as t :refer [is testing]]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-publisher]]
            [grafter-2.rdf.protocols :refer [->Quad]]
            [grafter-2.rdf4j.repository :as repo]
            [drafter.feature.draftset.create-test :as ct]
            [clojure.string :as string])
  (:import java.net.URI
           java.util.UUID))

(t/use-fixtures :each tc/with-spec-instrumentation)

(def system-config "test-system.edn")

(def keys-for-test
  [[:drafter/routes :draftset/api]
   :drafter/write-scheduler
   :drafter.routes.sparql/live-sparql-query-route
   :drafter.backend.live/endpoint
   :drafter.common.config/sparql-query-endpoint])

(def dg-q
  "SELECT ?o WHERE { GRAPH ?g { <%s> <http://publishmydata.com/def/drafter/hasDraft> ?o } }")

(defn draft-graph-uri-for [conn graph-uri]
  (:o (first (repo/query conn (format dg-q graph-uri)))))

(tc/deftest-system-with-keys append-draft-graph-in-non-graph-position-rewrite-test
  keys-for-test [system system-config]
  (let [handler (get system [:drafter/routes :draftset/api])
        live    (:drafter.routes.sparql/live-sparql-query-route system)
        draftset-location (help/create-draftset-through-api handler test-publisher)
        live-graph-uri (URI. (str "http://live-graph/" (UUID/randomUUID)))
        quad-1 (->Quad live-graph-uri
                       (URI. "http://www.w3.org/1999/02/22-rdf-syntax-ns#a")
                       (URI. "http://example.org/animals/kitten")
                       live-graph-uri)
        quad-2 (->Quad (URI. "http://uri-1")
                       (URI. "http://www.w3.org/1999/02/22-rdf-syntax-ns#a")
                       live-graph-uri
                       live-graph-uri)
        live-graph-uri-2 (URI. (str "http://live-graph/" (UUID/randomUUID)))
        quad-3 (->Quad live-graph-uri-2
                       (URI. "http://www.w3.org/1999/02/22-rdf-syntax-ns#a")
                       (URI. "http://uri-2")
                       live-graph-uri-2)
        quads [quad-1 quad-2 quad-3]
        endpoint (:drafter.common.config/sparql-query-endpoint system)]
    (help/append-quads-to-draftset-through-api handler test-publisher draftset-location quads)
    (with-open [conn (repo/->connection (repo/sparql-repo endpoint))]
      (let [draft-graph-q (-> "SELECT ?o WHERE { GRAPH ?g { <%s> <http://publishmydata.com/def/drafter/hasDraft> ?o } }"
                              (format live-graph-uri))
            draft-graph-uri (:o (first (repo/query conn draft-graph-q)))]
        (testing "The graph in ?s position has been rewritten"

          (is (->> draft-graph-uri
                   (format "ASK { <%s> ?p <http://example.org/animals/kitten> }")
                   (repo/query conn)))

          (is (->> draft-graph-uri
                   (format "ASK { <http://uri-1> ?p <%s> }")
                   (repo/query conn))))

        (testing "We can still query for the live graph URI in ?s position"
          (let [q (format "ASK { <%s> ?p <http://example.org/animals/kitten> }" live-graph-uri)
                req (tc/with-identity test-publisher
                      {:uri (str draftset-location "/query")
                       :request-method :post
                       :headers {"accept" "application/sparql-results+json"
                                 "content-type" "application/sparql-query"}
                       :body (java.io.ByteArrayInputStream. (.getBytes q))})
                res (handler req)]
            (is (= true (:boolean (json/parse-string (:body res) keyword))))))

        (testing "When deleting a triple, it's gone"
          (help/delete-quads-through-api handler
                                         test-publisher
                                         draftset-location
                                         [quad-2])

          ;; Triple is not present with draft-graph uri in ?o position
          (is (false? (->> draft-graph-uri
                           (format "ASK { <http://uri-1> ?p <%s> }")
                           (repo/query conn))))

          ;; Triple is not present with live-graph uri in ?o position
          (is (false? (->> live-graph-uri
                           (format "ASK { <http://uri-1> ?p <%s> }")
                           (repo/query conn))))

          (help/delete-quads-through-api handler
                                         test-publisher
                                         draftset-location
                                         [quad-3])

          ;; Triple is not present in draftset quads
          (is (not (some #(= (:o quad-3) (:o %))
                         (help/get-draftset-quads-through-api handler draftset-location test-publisher)))))

        (testing "Graph metadata reports graph deleted where last triple is deleted"
          (is (= :deleted (get-in (help/get-draftset-info-through-api handler draftset-location test-publisher)
                                  [:changes live-graph-uri-2 :status]))))

        (testing "When publishing, draft-graph-uris are written back"
          (help/publish-draftset-through-api handler draftset-location test-publisher)

          ;; triple is back with the live graph in ?s position
          (is (->> live-graph-uri
                   (format "ASK { <%s> ?p <http://example.org/animals/kitten> }")
                   (repo/query conn)))

          ;; and the draft graph is not in thes ?s position
          (is (false? (->> draft-graph-uri
                           (format "ASK { <%s> ?p ?o }")
                           (repo/query conn))))

          ;; and we can query it from the live endpoint
          (let [q (format "ASK { <%s> ?p <http://example.org/animals/kitten> }" live-graph-uri)
                req {:uri "/v1/sparql/live"
                     :request-method :post
                     :headers {"accept" "application/sparql-results+json"
                               "content-type" "application/sparql-query"}
                     :body (java.io.ByteArrayInputStream. (.getBytes q))}
                res (live req)]
            (is (true? (:boolean (json/parse-string (:body res) keyword))))))

        (testing "After publish, triple deleted from draft is not present"
          ;; Triple is not in stardog
          (is (false? (->> live-graph-uri
                           (format "ASK { <http://uri-1> ?p <%s> }")
                           (repo/query conn))))

          ;; And can't be accessed in the live drafter endpoint
          (let [q (format "ASK { <http://uri-1> ?p <%s> }" live-graph-uri)
                req {:uri "/v1/sparql/live"
                     :request-method :post
                     :headers {"accept" "application/sparql-results+json"
                               "content-type" "application/sparql-query"}
                     :body (java.io.ByteArrayInputStream. (.getBytes q))}
                res (live req)]
            (is (false? (:boolean (json/parse-string (:body res) keyword))))))))))

(tc/deftest-system-with-keys live-graph-in-data-rewriting-delete-test
  keys-for-test [system system-config]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    (let [handler (get system [:drafter/routes :draftset/api])
          live    (:drafter.routes.sparql/live-sparql-query-route system)
          draftset-location (help/create-draftset-through-api handler test-publisher)
          live-graph-uri-1 (URI. (str "http://live-graph/" (UUID/randomUUID)))
          live-graph-uri-2 (URI. (str "http://live-graph/" (UUID/randomUUID)))
          quad-1 (->Quad live-graph-uri-1
                         (URI. "http://www.w3.org/1999/02/22-rdf-syntax-ns#a")
                         live-graph-uri-2
                         live-graph-uri-1)
          quad-2 (->Quad (URI. "http://uri-1")
                         (URI. "http://www.w3.org/1999/02/22-rdf-syntax-ns#a")
                         live-graph-uri-2
                         live-graph-uri-2)
          quads [quad-1]
          _ (help/append-quads-to-draftset-through-api handler test-publisher draftset-location [quad-1 quad-2])
          ;; _ (help/append-quads-to-draftset-through-api handler test-publisher draftset-location [quad-2])
          draft-graph-uri-1 (:o (first (repo/query conn (format dg-q live-graph-uri-1))))
          draft-graph-uri-2 (:o (first (repo/query conn (format dg-q live-graph-uri-2))))]

      (testing "Draft graph 1 & 2 were rewritten"

        (is (false? (->> (format "ASK { GRAPH <%s> { <%s> ?p <%s> } }"
                                 live-graph-uri-1
                                 live-graph-uri-1
                                 live-graph-uri-2)
                         (repo/query conn))))

        (is (true? (->> (format "ASK { GRAPH <%s> { <%s> ?p <%s> } }"
                                draft-graph-uri-1
                                draft-graph-uri-1
                                draft-graph-uri-2)
                        (repo/query conn))))

        (is (true? (->> (format "ASK { GRAPH <%s> { <http://uri-1> ?p <%s> } }"
                                draft-graph-uri-2
                                draft-graph-uri-2)
                        (repo/query conn)))))

      ;; When we delete a draft graph, references to it should be rewritten back
      ;; to the live uri
      (help/delete-draftset-graph-through-api
       handler test-publisher draftset-location live-graph-uri-2)

      (testing "Deleted graph triples are gone"
        (is (false? (->> (format "ASK { GRAPH <%s> { <http://uri-1> ?p <%s> } }"
                                 draft-graph-uri-2
                                 draft-graph-uri-2)
                         (repo/query conn)))))

      (testing "Draft graph 2 was rewritten back to live"

        (is (false? (->> (format "ASK { GRAPH <%s> { <%s> ?p <%s> } }"
                                 draft-graph-uri-1
                                 draft-graph-uri-1
                                 draft-graph-uri-2)
                         (repo/query conn))))

        (is (true? (->> (format "ASK { GRAPH <%s> { <%s> ?p <%s> } }"
                                draft-graph-uri-1
                                draft-graph-uri-1
                                live-graph-uri-2)
                        (repo/query conn)))))

      (testing "Graph metadata reports graph deleted where last triple is deleted"
        (is (= :deleted (get-in (help/get-draftset-info-through-api
                                 handler draftset-location test-publisher)
                                [:changes live-graph-uri-2 :status])))))))

(tc/deftest-system-with-keys copy-live-graph-into-draftset-test
  keys-for-test [system system-config]
  (with-open [conn (-> system
                       :drafter.common.config/sparql-query-endpoint
                       repo/sparql-repo
                       repo/->connection)]
    ;; Setup
    (let [handler (get system [:drafter/routes :draftset/api])
          live    (:drafter.routes.sparql/live-sparql-query-route system)
          draftset-location (help/create-draftset-through-api handler test-publisher)
          graph-to-copy-uri (URI. (str "http://live-graph/" (UUID/randomUUID)))
          quad-in-copy-graph (->Quad graph-to-copy-uri
                                     (URI. "http://p")
                                     (URI. "http://o")
                                     graph-to-copy-uri)
          ;; add quad to graph
          _ (help/append-quads-to-draftset-through-api
             handler test-publisher draftset-location [quad-in-copy-graph])
          ;; and publish to live
          _ (help/publish-draftset-through-api handler draftset-location test-publisher)

          ;; new draftset, old one is gone
          draftset-location (help/create-draftset-through-api handler test-publisher)
          draftset-id (last (string/split draftset-location #"/"))
          live-graph-uri-1 (URI. (str "http://live-graph/" (UUID/randomUUID)))
          quad-1 (->Quad live-graph-uri-1
                         (URI. "http://p")
                         graph-to-copy-uri
                         live-graph-uri-1)

          ;; add quad referencing quad-in-copy-graph to ds
          _ (help/append-quads-to-draftset-through-api
             handler test-publisher draftset-location [quad-1])
          draft-graph-uri-1 (draft-graph-uri-for conn live-graph-uri-1)

          copy-graph-request (tc/with-identity test-publisher
                               {:uri (str draftset-location "/graph")
                                :request-method :put
                                :params {:draftset-id draftset-id
                                         :graph (str graph-to-copy-uri)}})
          copy-graph-response (handler copy-graph-request)
          _ (tc/await-success (get-in copy-graph-response [:body :finished-job]))
          draft-graph-to-copy-uri (draft-graph-uri-for conn graph-to-copy-uri)

          live-graph-uri-2 (URI. (str "http://live-graph/" (UUID/randomUUID)))
          quad-2 (->Quad graph-to-copy-uri
                         (URI. "http://p")
                         live-graph-uri-2
                         live-graph-uri-2)
          ;; add quad referencing quad-in-copy-graph to ds
          _ (help/append-quads-to-draftset-through-api
             handler test-publisher draftset-location [quad-2])
          draft-graph-uri-2 (draft-graph-uri-for conn live-graph-uri-2)]

      (testing "Has `graph-to-copy-uri` been rewritten in itself?"
        (is (true? (->> (format "ASK { GRAPH <%s> { <%s> <http://p> <http://o> } }"
                                 draft-graph-to-copy-uri draft-graph-to-copy-uri)
                        (repo/query conn)))))

      (testing "Has `graph-to-copy-uri` been rewritten in `quad-1`?"
        (is (false? (->> (format "ASK { GRAPH <%s> { <%s> <http://p> <%s> } }"
                                 draft-graph-uri-1 draft-graph-uri-1 graph-to-copy-uri)
                         (repo/query conn))))

        (is (true? (->> (format "ASK { GRAPH <%s> { <%s> <http://p> <%s> } }"
                                draft-graph-uri-1 draft-graph-uri-1 draft-graph-to-copy-uri)
                        (repo/query conn)))))

      (testing "Has `graph-to-copy-uri` been rewritten in `quad-2`?"
        (is (false? (->> (format "ASK { GRAPH <%s> { <%s> <http://p> <%s> } }"
                                 draft-graph-uri-2 graph-to-copy-uri draft-graph-uri-2)
                         (repo/query conn))))

        (is (true? (->> (format "ASK { GRAPH <%s> { <%s> <http://p> <%s> } }"
                                draft-graph-uri-2 draft-graph-to-copy-uri draft-graph-uri-2)
                        (repo/query conn)))))

      (help/delete-draftset-graph-through-api
       handler test-publisher draftset-location live-graph-uri-1)

      (testing "Deleted graph triples are gone"
        (is (false? (->> (format "ASK { GRAPH <%s> { ?s ?p ?o } }"
                                 draft-graph-uri-1)
                         (repo/query conn)))))

      (testing "Graph metadata reports graph deleted where last triple is deleted"
        (is (= :deleted (get-in (help/get-draftset-info-through-api
                                 handler draftset-location test-publisher)
                                [:changes live-graph-uri-1 :status]))))

      (help/delete-quads-through-api
       handler test-publisher draftset-location [quad-in-copy-graph])

      (help/publish-draftset-through-api handler draftset-location test-publisher)

      (testing "After publish triple copied from live, then deleted, is gone"
        (is (false? (->> (format "ASK { GRAPH <%s> { ?s ?p ?o } }"
                                 draft-graph-to-copy-uri)
                         (repo/query conn)))))

      (testing "After publish, triple copied from live, then deleted, is rewritten back to live"
          (let [[{:keys [s o]}] (->> live-graph-uri-2
                                  (format "SELECT * { GRAPH <%s> { ?s ?p ?o } }")
                                  (repo/query conn))]
         (is (= graph-to-copy-uri s))
         (is (= live-graph-uri-2 o)))))))
