(ns drafter.routes.dumps-test
  (:require [clojure.test :refer :all]
            [drafter.routes.dumps :refer :all]
            [clojure.template :refer [do-template]]
            [drafter.routes.sparql :refer [draft-sparql-routes]]
            [drafter.rdf.sparql-protocol :refer [sparql-end-point]]
            [grafter.rdf.formats :refer :all]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.io :refer [rdf-serializer]]
            [drafter.rdf.draft-management :refer [migrate-live!]]
            [drafter.test-common :refer [wrap-clean-test-db wrap-db-setup
                                         *test-backend* test-triples import-data-to-draft!
                                         stream->string select-all-in-graph]]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :each validate-schemas)

(def dumps-request {:request-method :get
                    :uri "/data/live"
                    :params {:graph-uri "http://capybara.com/capybara-data-1"}
                    :headers {"accept" "application/n-triples"}})

(defn make-store-with-draft []
  (let [draft-graph (import-data-to-draft! *test-backend* "http://capybara.com/capybara-data-1" (test-triples "http://test.com/subject-1"))]
    draft-graph))

(defn count-statements [response-data]
  (count (statements (:body response-data)
                     :format rdf-ntriples)))

(deftest dumps-route-raw-test
  (testing "dumps-endpoint with live endpoint"
    (let [draft-graph (make-store-with-draft)]
      (migrate-live! *test-backend* draft-graph)
      (let [dumps (dumps-endpoint "/data/live" sparql-end-point *test-backend*)
            response (dumps dumps-request)]

        (is (= 2 (count-statements response)))
        (is (= "attachment; filename=\"capybara-data-1.nt\"" (get-in response [:headers "Content-Disposition"])))))))

(deftest dumps-route-draft-test
  (testing "dumps-endpoint with draft endpoint"
    (let [draft-graph (make-store-with-draft)]

      (let [dumps (dumps-endpoint "/data/live" draft-sparql-routes *test-backend*)
            response (dumps (assoc-in dumps-request [:params :graph] draft-graph))]

        (is (= 2 (count-statements response)))))))

(use-fixtures :once wrap-db-setup)
(use-fixtures :each wrap-clean-test-db)
