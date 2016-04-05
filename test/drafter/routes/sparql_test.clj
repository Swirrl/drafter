(ns drafter.routes.sparql-test
  (:require [drafter.test-common :refer [throws-exception? test-triples import-data-to-draft!
                                         stream->string select-all-in-graph make-graph-live!
                                         *test-backend* wrap-clean-test-db wrap-db-setup]]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure-csv.core :as csv]
            [clojure.data.json :as json]
            [grafter.rdf :refer [subject predicate object context]]
            [grafter.rdf.repository :as repo]
            [grafter.rdf.protocols :as pr]
            [drafter.util :refer [to-coll]]
            [drafter.routes.sparql :refer :all]
            [drafter.rdf.draft-management :refer :all]
            [swirrl-server.errors :refer [encode-error]]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :each validate-schemas)

(defn add-test-data!
  "Set the state of the database so that we have three managed graphs,
  one of which is made public the other two are still private (draft)."
  [db]
  (let [draft-made-live-and-deleted (import-data-to-draft! db "http://test.com/made-live-and-deleted-1" (test-triples "http://test.com/subject-1"))
        draft-2 (import-data-to-draft! db "http://test.com/graph-2" (test-triples "http://test.com/subject-2"))
        draft-3 (import-data-to-draft! db "http://test.com/graph-3" (test-triples "http://test.com/subject-3"))]
    (migrate-live! db draft-made-live-and-deleted)
    [draft-made-live-and-deleted draft-2 draft-3]))

(def graph-1-result ["http://test.com/subject-1" "http://test.com/hasProperty" "http://test.com/data/1"])
(def graph-2-result ["http://test.com/subject-2" "http://test.com/hasProperty" "http://test.com/data/1"])

(def default-sparql-query {:request-method :get
                           :uri "/sparql/live"
                           :params {:query "SELECT * WHERE { ?s ?p ?o }"}
                           :headers {"accept" "text/csv"}})

(defn- build-query [endpoint-path query graphs]
  (let [graphs (to-coll graphs)
        query-request (-> default-sparql-query
                          (assoc-in [:params :query] query)
                          (assoc :uri endpoint-path))]

    (reduce (fn [req graph]
              (log/spy (update-in req [:params :graph] (fn [old new]
                                                         (cond
                                                          (nil? old) new
                                                          (instance? String old) [old new]
                                                          :else (conj old new))) graph)))
            query-request graphs)))

(def draft-query (partial build-query "/sparql/draft" ))

(defn live-query [qstr]
  (build-query "/sparql/live" qstr nil))

(defn state-query [qstr]
  (build-query "/sparql/state" qstr nil))

(defn raw-query [qstr]
  (build-query "/sparql/raw" qstr nil))

(defn csv-> [{:keys [body]}]
  "Parse a response into a CSV"
  (-> body stream->string csv/parse-csv))

(deftest live-sparql-routes-test
  (let [[draft-graph-1 draft-graph-2] (add-test-data! *test-backend*)
        endpoint (live-sparql-routes "/sparql/live" *test-backend* nil)
        {:keys [status headers body]
         :as result} (endpoint (live-query (select-all-in-graph "http://test.com/made-live-and-deleted-1")))
        csv-result (csv-> result)]

    (is (= "text/csv" (headers "Content-Type"))
        "Returns content-type")

    (is (= ["s" "p" "o"] (first csv-result))
        "Returns CSV")

    (is (= graph-1-result (second csv-result))
        "Named (live) graph is publicly queryable")

    (testing "Draft graphs are not exposed"
      (let [csv-result (csv-> (endpoint
                               (live-query (select-all-in-graph draft-graph-2))))]
        (is (empty? (second csv-result)))))

    (testing "Offline public graphs are not exposed"
      (set-isPublic! *test-backend* "http://test.com/made-live-and-deleted-1" false)
      (let [csv-result (csv-> (endpoint
                               (live-query
                                (select-all-in-graph "http://test.com/made-live-and-deleted-1"))))]

        (is (not= graph-1-result (second csv-result)))))))

(deftest state-sparql-routes-test
  (let [;;drafts-request (assoc-in [:headers "accept"] "text/plain; charset=utf-8")
        [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! *test-backend*)
        endpoint (state-sparql-routes "/sparql/state" *test-backend* nil)]

    (testing "The state graph should be accessible"
      (let [result (endpoint (-> (state-query (str "ASK WHERE {"
                                                   "  GRAPH <" drafter-state-graph "> {"
                                                   "    ?s ?p ?o ."
                                                   "  }"
                                                   "}"))
                                 (assoc-in [:headers "accept"] "text/plain; charset=utf-8")))
            body (-> result :body stream->string)]

        (is (= "true" body))))

    (testing "The data graphs (live and drafts) should be hidden"
      (let [result (endpoint
                    (-> (state-query (str "ASK WHERE {"
                                          "  GRAPH <" draft-graph-2 "> {"
                                          "    ?s ?p ?o ."
                                          "  }"
                                          "}"))
                        (assoc-in [:headers "accept"] "text/plain; charset=utf-8")))
            body (-> result :body stream->string)]

        (is (= "false" body))))))

(deftest raw-sparql-routes-test
  (let [;;drafts-request (assoc-in [:headers "accept"] "text/plain; charset=utf-8")
        [draft-graph-1 draft-graph-2 draft-graph-3] (add-test-data! *test-backend*)
        endpoint (raw-sparql-routes "/sparql/raw" *test-backend* nil)]

    (testing "The state graph should be accessible"
      (let [result (endpoint (-> (raw-query (str "ASK WHERE {"
                                                   "  GRAPH <" drafter-state-graph "> {"
                                                   "    ?s ?p ?o ."
                                                   "  }"
                                                   "}"))
                                 (assoc-in [:headers "accept"] "text/plain; charset=utf-8")))
            body (-> result :body stream->string)]

        (is (= "true" body))))

    (testing "The data graphs (live and drafts) should be accessible"
      (let [result (endpoint
                     (-> (raw-query (str "ASK WHERE {"
                                           "  GRAPH <" draft-graph-2 "> {"
                                           "    ?s ?p ?o ."
                                           "  }"
                                           "}"))
                         (assoc-in [:headers "accept"] "text/plain; charset=utf-8")))
            body (-> result :body stream->string)]

        (is (= "true" body))))))

(defn make-new-draft-from-graph! [backend live-guri]
  (let [draft-guri (create-draft-graph! backend live-guri)
        query-str (str "CONSTRUCT { ?s ?p ?o } WHERE
                         { GRAPH <" live-guri "> { ?s ?p ?o } }")
        source-data (repo/query backend query-str)]
    (append-data-batch! backend draft-guri source-data)

    draft-guri))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each (partial wrap-clean-test-db))
