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
  (let [q-size 1
        queue (q/make-queue q-size)]

    (testing "POST /draft/create"
      (testing "without a live-graph param returns a 400 error"
        (let [{:keys [status body headers]} ((api-routes *test-db* queue)
                                             {:uri "/draft/create"
                                              :request-method :post
                                              :headers {"accept" "application/json"}})]
          (is (= 400 status))
          (is (= :error (body :type)))
          (is (instance? String (body :msg)))))

      (testing (str "with live-graph=" test-graph-uri " should create a new managed graph and draft")

        (let [{:keys [status body headers]} ((api-routes *test-db* queue)
                                             {:uri "/draft/create"
                                              :request-method :post
                                              :query-params {"live-graph" test-graph-uri}
                                              :headers {"accept" "application/json"}})]
          (is (= 201 status))
          (is (url? (-> body :guri))))))

    (testing "POST /draft"
      (let [test-request {:uri "/draft"
                          :request-method :post
                          :query-params {"graph" "http://draft.org/draft-graph"}
                          :params {:file {:filename "test.nt"
                                          :tempfile :tempfile-here
                                          :size 10}}}

            route (api-routes *test-db* queue)
            {:keys [status body headers]} (route test-request)]

        (testing "returns job id"
          (is (= 202 status))
          (is (instance? java.util.UUID (:queue-id body)))
          (is (instance? String (:msg body)))
          (is (= :ok (:type body))))

        (testing "adds append job to queue"
          (is (= 1 (q/size queue)))

          (let [job-id (:queue-id body)
                job (q/find-job queue job-id)]

            (is (= {:tempfile :tempfile-here
                    :filename "test.nt"
                    :size 10
                    :action :append-file
                    :graph-uri "http://draft.org/draft-graph"
                    :id job-id}

                   job))))

        (testing "full queue returns a 503 service temporarily unavailable"
          ;; Make a second request.  We should now 503 because the
          ;; queue is full as q-size == 1.
          (let [{:keys [status body headers]} (route test-request)]
            (is (= 503 status))
            (is (= :error (:type body)))
            (is (instance? String (:msg body))))))))

  (testing "PUT /draft"
    (let [queue (q/make-queue 10)
          test-request {:uri "/draft"
                        :request-method :put
                        :query-params {"graph" "http://draft.org/draft-graph"}
                        :params {:file {:filename "test.nt"
                                        :tempfile :tempfile-here
                                        :size 10}}}

          route (api-routes *test-db* queue)
          {:keys [status body headers]} (route test-request)]

      (testing "adds replace job to queue"
        (let [job-id (:queue-id body)
              job (q/find-job queue job-id)]

          (is (= {:tempfile :tempfile-here
                  :filename "test.nt"
                  :size 10
                  :action :replace-with-file
                  :graph-uri "http://draft.org/draft-graph"
                  :id job-id}

                 job))))))

  ;; TODO add tests for DELETE
  ;; TODO add tests for migrate

  )

(use-fixtures :each wrap-with-clean-test-db)
