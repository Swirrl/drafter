(ns drafter.routes.dumps-test
  (:require [clojure.test :refer :all]
            [drafter.routes.dumps :refer :all]
            [grafter.rdf.formats :refer :all]
            [grafter.rdf :refer [statements]]
            [grafter.rdf.io :refer [rdf-serializer]]
            [drafter.rdf.draft-management :refer [import-data-to-draft! migrate-live!]]
            [drafter.test-common :refer [test-triples
                                         make-store stream->string select-all-in-graph make-graph-live!]]))

(def dumps-request {:request-method :get
                    :uri "/data/live"
                    :params {"graph-uri" "http://capybara.com/1"}
                    :headers {"accept" "application/n-triples"}})

(deftest dumps-route-test
  (let [test-db (make-store)
        draft-graph (import-data-to-draft! test-db "http://capybara.com/1" (test-triples "http://test.com/subject-1"))]
    (migrate-live! test-db draft-graph)

    (let [dumps (dumps-route "/data/live" test-db)
          response-data (:body (dumps dumps-request))]

      (is (= 2 (count (statements response-data
                                  :format rdf-ntriples)))))))
