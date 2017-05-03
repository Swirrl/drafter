(ns drafter.routes.sparql-test
  (:require [clojure-csv.core :as csv]
            [clojure.test :refer :all]
            [drafter
             [test-common :refer [*test-backend* select-all-in-graph stream->string wrap-clean-test-db wrap-db-setup]]
             [timeouts :as timeouts]]
            [drafter.rdf.draft-management :refer :all]
            [drafter.routes.sparql :refer :all]
            [drafter.test-generators :as gen]
            [schema.test :refer [validate-schemas]])
  (:import [java.net URI]))

(use-fixtures :each validate-schemas)

(def default-sparql-query {:request-method :get
                           :uri "/sparql/live"
                           :params {:query "SELECT * WHERE { ?s ?p ?o }"}
                           :headers {"accept" "text/csv"}})

(defn- build-query
  [endpoint-path query]
  (-> default-sparql-query
      (assoc-in [:params :query] query)
      (assoc :uri endpoint-path)))

(defn live-query [qstr]
  (let [endpoint (live-sparql-routes "/sparql/live" *test-backend* timeouts/calculate-default-request-timeout)
        request (build-query "/sparql/live" qstr)]
    (endpoint request)))

(defn csv-> [{:keys [body]}]
  "Parse a response into a CSV"
  (-> body stream->string csv/parse-csv))

(deftest can-query-live-graphs-test
  (let [live-graph-uri (URI. "http://live")
        live-triples (gen/generate-triples 1 10)
        expected-rows (map #(map str %) (map (juxt :s :p :o) live-triples))]
    (gen/generate-in *test-backend* {:managed-graphs {live-graph-uri {:is-public true
                                                                      :triples live-triples
                                                                      :drafts ::gen/gen}}})
    (let [{:keys [headers] :as response} (live-query (select-all-in-graph live-graph-uri))
          csv-result (csv-> response)]
      (is (= "text/csv" (headers "Content-Type"))
          "Returns content-type")

      (is (= ["s" "p" "o"] (first csv-result)) "Returns CSV")

      (is (= (set expected-rows) (set (rest csv-result)))))))

(deftest cannot-query-draft-graphs-through-live-endpoint-test
  (let [live-graph-uri (URI. "http://live")
        draft-graph-uri (URI. "http://draft")]
    (gen/generate-in *test-backend* {:draftsets 1
                                     :managed-graphs {live-graph-uri {:is-public true
                                                                      :triples ::gen/gen
                                                                      :drafts {draft-graph-uri {:triples 10}}}}})

    (let [{:keys [headers] :as response} (live-query (select-all-in-graph draft-graph-uri))
          csv-result (csv-> response)]
      (is (empty? (rest csv-result))))))

(deftest cannot-query-non-public-graphs-through-live-endpoint-test
  (let [managed-graph-uri (URI. "http://non-public")]
    (gen/generate-in *test-backend* {:managed-graphs {managed-graph-uri {:is-public false
                                                                         :triples 10}}})

    (let [response (live-query (select-all-in-graph managed-graph-uri))
          csv-result (csv-> response)]
      (is (= ["s" "p" "o"] (first csv-result)))
      (is (empty? (rest csv-result))))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each (partial wrap-clean-test-db))
