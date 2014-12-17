(ns drafter.routes.dumps-test
  (:require [clojure.test :refer :all]
            [drafter.routes.dumps :refer :all]
            [drafter.rdf.draft-management :refer [import-data-to-draft!]]
            [drafter.test-common :refer [test-triples
                                         make-store stream->string select-all-in-graph make-graph-live!]]))

(def dumps-request {:request-method :get
                    :uri "/data/live"
                    :params {:graph-uri "http://capybara.com/1"}
                    :headers {"accept" "application/n-triples"}})

(deftest dumps-route-test
  (let [test-db (make-store)]
    (import-data-to-draft! test-db "http://capybara.com/1" (test-triples "http://test.com/subject-1"))


    (let [dumps (dumps-route "/data/live" test-db)]
      (is (= true (dumps dumps-request))))


    ))
