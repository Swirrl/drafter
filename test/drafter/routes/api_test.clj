(ns drafter.routes.api-test
  (:require [drafter.test-common :refer [*test-db* test-triples wrap-with-clean-test-db
                                         stream->string select-all-in-graph]]
            [clojure.test :refer :all]
            [grafter.rdf.sesame :as ses]
            [drafter.routes.api :refer :all]
            [drafter.rdf.queue :as q]
            [drafter.rdf.draft-management :refer :all]))

(def test-graph-uri "http://example.org/my-graph")

(defn url? [u]
  (try
    (java.net.URL. u)
    true
    (catch Exception ex
      false)))

(deftest api-routes-test
  (let [queue (q/make-queue 2)]
    (testing "POST /draft/create without a live-graph param returns a 400 error"
      (let [{:keys [status body headers]} ((api-routes *test-db* queue)
                                           {:uri "/draft/create"
                                            :request-method :post
                                            :headers {"accept" "application/json"}})]
        (is (= 400 status))
        (is (= :error (body :type)))
        (is (instance? String (body :msg)))))

    (testing (str "POST /draft/create?live-graph=" test-graph-uri " should create a new managed graph and draft.")

      (let [{:keys [status body headers]} ((api-routes *test-db* queue)
                                           {:uri "/draft/create"
                                            :request-method :post
                                            :query-params {"live-graph" test-graph-uri}
                                            :headers {"accept" "application/json"}})]
        (is (= 201 status))
        (is (url? (-> body :guri)))))


    ;; TODO add tests for import/replace & queue

    ;; TODO add tests for DELETE

    ))

(use-fixtures :each wrap-with-clean-test-db)
