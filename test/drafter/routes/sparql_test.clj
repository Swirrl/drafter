(ns drafter.routes.sparql-test
  (:require [clojure-csv.core :as csv]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [drafter.backend.draftset.draft-management :refer :all]
            [drafter.rdf.drafter-ontology :refer :all]
            [drafter.routes.sparql :refer :all]
            [drafter.test-common
             :refer
             [*test-backend*
              assert-is-forbidden-response
              import-data-to-draft!
              select-all-in-graph
              stream->string
              test-triples
              with-identity
              wrap-system-setup
              deftest-system]]
            [drafter.timeouts :as timeouts]
            [drafter.user-test :refer [test-editor test-system]]
            [schema.test :refer [validate-schemas]])
  (:import java.net.URI))

(use-fixtures :each (join-fixtures [validate-schemas]))

(def draft-graph-1 (draft:graph "dg-1"))
(def draft-graph-2 (draft:graph "dg-2"))

(def graph-1-result [(URI. "http://test.com/subject-1") (URI. "http://test.com/hasProperty") (URI. "http://test.com/data/1")])

(def default-sparql-query {:request-method :get
                           :uri "/sparql/live"
                           :query-params {"query" "SELECT * WHERE { ?s ?p ?o }"}
                           :headers {"accept" "text/csv"}})

(defn- build-query
  ([endpoint-path query] (build-query endpoint-path query))
  ([endpoint-path query graphs]
   (let [query-request (-> default-sparql-query
                           (assoc-in [:query-params "query"] query)
                           (assoc :uri endpoint-path))]

     (reduce (fn [req graph]
               (log/spy (update-in req [:params :graph] (fn [old new]
                                                          (cond
                                                            (nil? old) new
                                                            (instance? String old) [old new]
                                                            :else (conj old new))) graph)))
             query-request graphs))))

(defn live-query [qstr]
  (build-query "/v1/sparql/live" qstr nil))

(defn csv-> [{:keys [body]}]
  "Parse a response into a CSV"
  (-> body stream->string csv/parse-csv))

(deftest-system live-sparql-routes-test
  [{endpoint :drafter.routes.sparql/live-sparql-query-route
    :keys [drafter.stasher/repo] :as system} "drafter/routes/sparql-test/system.edn"]
  (let [{:keys [status headers body]
         :as result} (endpoint (live-query (select-all-in-graph "http://test.com/made-live-and-deleted-1")))
        csv-result (csv-> result)]

    (is (= "text/csv" (headers "Content-Type"))
        "Returns content-type")

    (is (= ["s" "p" "o"] (first csv-result))
        "Returns CSV")

    (is (= graph-1-result (map #(URI. %) (second csv-result)))
        "Named (live) graph is publicly queryable")

    (testing "Draft graphs are not exposed"
      (let [csv-result (csv-> (endpoint
                               (live-query (select-all-in-graph draft-graph-2))))]
        (is (empty? (second csv-result)))))

    (testing "Offline public graphs are not exposed"
      (set-isPublic! repo "http://test.com/made-live-and-deleted-1" false)
      (let [csv-result (csv-> (endpoint
                               (live-query
                                (select-all-in-graph "http://test.com/made-live-and-deleted-1"))))]
        (is (not= graph-1-result (second csv-result)))))))



