(ns ^:rest-api drafter.feature.draftset.query-test
  (:require [clojure.test :as t :refer [is use-fixtures join-fixtures]]
            [drafter.test-common :as tc]
            [drafter.user-test :refer [test-editor]]
            [drafter.routes.draftsets-api-test
             :refer [create-draftset-through-api setup-route]]))

(use-fixtures :each
  (join-fixtures [(tc/wrap-system-setup "test-system.edn"
                                        [:drafter.user/repo
                                         :drafter.routes/draftsets-api
                                         :drafter.backend/rdf4j-repo
                                         :drafter/write-scheduler])
                  setup-route]))

(defn- create-query-request [user draftset-location query accept-content-type & {:keys [union-with-live?]}]
  (tc/with-identity user
    {:uri (str draftset-location "/query")
     :headers {"accept" accept-content-type}
     :request-method :post
     :params {:query query :union-with-live union-with-live?}}))

(tc/deftest-system-with-keys query-draftset-disallowed-with-service-query
  [:drafter.user/repo
   :drafter.routes/draftsets-api
   :drafter.backend/rdf4j-repo
   :drafter/write-scheduler]
  [system "test-system.edn"]
  (let [handler (get system :drafter.routes/draftsets-api)
        draftset-location (create-draftset-through-api test-editor)]
    (let [query-request (create-query-request test-editor
                                              draftset-location
                                              "SELECT * WHERE { SERVICE <http://anything> { ?s ?p ?o } }" "application/sparql-results+json")
          query-response (handler query-request)]
      (tc/assert-is-bad-request-response query-response))
    (let [query-request (create-query-request test-editor draftset-location "
SELECT * WHERE {
  GRAPH ?g { ?s ?p ?o }
  GRAPH ?g {
    SERVICE <db:somedb> {
      { ?s ?p ?o }
    }
  }
}"  "application/sparql-results+json")
          query-response (handler query-request)]
      (tc/assert-is-bad-request-response query-response))))
