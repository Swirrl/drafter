(ns drafter.routes.results-table-test
  (:require [clojure.test :refer :all]
            [drafter.routes.results-table :refer :all]
;;             [clojure.template :refer [do-template]]
;;             [drafter.routes.sparql :refer [draft-sparql-routes]]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point]]
;;             [grafter.rdf.formats :refer :all]
;;             [grafter.rdf :refer [statements]]
;;             [grafter.rdf.io :refer [rdf-serializer]]
             [drafter.rdf.draft-management :refer [import-data-to-draft! migrate-live!]]
             [drafter.test-common :refer [test-triples make-store]]
;;                                          make-store stream->string select-all-in-graph make-graph-live!]]
             [grafter.tabular :refer [read-dataset make-dataset move-first-row-to-header column-names]]
            ))

;; (def dumps-request {:request-method :get
;;                     :uri "/data/live"
;;                     :params {:graph-uri "http://capybara.com/capybara-data-1"}
;;                     :headers {"accept" "application/n-triples"}})

;; (defn make-store-with-draft []
;;   (let [test-db (make-store)
;;         draft-graph (import-data-to-draft! test-db "http://capybara.com/capybara-data-1" (test-triples "http://test.com/subject-1"))]
;;     [test-db draft-graph]))

;; (defn count-statements [response-data]
;;   (count (statements (:body response-data)
;;                      :format rdf-ntriples)))

(def results-table-request { :request-method :post
                             :uri "/results-table/live"
                             :params { :query "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10",
                                       :headers "subject,predicate,object" }
                             :headers { "accept" "text/csv" }})

(defn read-csv [csv]
  (-> csv (read-dataset :format :csv) (make-dataset move-first-row-to-header)))

(deftest results-table-route-live-test
  (testing "results-table on live endpoint"
    (let [[test-db draft-graph] (make-store-with-draft)]
      (migrate-live! test-db draft-graph)

      (let [results-table-route (results-table-endpoint "/results-table/live" sparql-end-point test-db)
            response (results-table-route results-table-request)
            dataset (read-csv (:body response))]

        (testing "has all of the rows"
          (is (= 4
                 (count (:rows dataset)))))

        (testing "has replaced column headers"
          (is (= ["subject" "predicate" "object"]
                 (column-names dataset))))))))

;; (deftest dumps-route-raw-test
;;   (testing "dumps-endpoint with live endpoint"
;;     (let [[test-db draft-graph] (make-store-with-draft)]
;;       (migrate-live! test-db draft-graph)

;;       (let [dumps (dumps-endpoint "/data/live" sparql-end-point test-db)
;;             response (dumps dumps-request)]

;;         (is (= 2 (count-statements response)))
;;         (is (= "attachment; filename=\"capybara-data-1.nt\"" (get-in response [:headers "Content-Disposition"])))))))

;; (deftest dumps-route-draft-test
;;   (testing "dumps-endpoint with draft endpoint"
;;     (let [[test-db draft-graph] (make-store-with-draft)]

;;       (let [dumps (dumps-endpoint "/data/live" draft-sparql-routes test-db)
;;             response (dumps (assoc-in dumps-request [:params :graph] draft-graph))]

;;         (is (= 2 (count-statements response)))))))
