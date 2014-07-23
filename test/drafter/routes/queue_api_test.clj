(ns drafter.routes.queue-api-test
  (:require [drafter.test-common :refer [*test-db* test-triples wrap-with-clean-test-db
                                         stream->string select-all-in-graph]]
            [clojure.test :refer :all]
            [grafter.rdf.sesame :as ses]
            [drafter.routes.queue-api :refer :all]
            [drafter.rdf.queue :as q]
            [clojure.java.io :as io]
            [drafter.rdf.draft-management :refer :all]))


(deftest queue-api-routes-test

  (let [q-size 10
        queue (q/make-queue q-size)]

  (testing "GET /queue/peek"
    (do (q/offer! queue identity {:desc "identity"})
        (q/offer! queue (fn foo[]) {:desc "anon"})) ; enqueue something

    (let [
          {:keys [status body headers]} ((queue-api-routes queue)
                                             {:uri "/queue/peek"
                                              :request-method :get
                                              :headers {"accept" "application/json"}})]

        (is (= 200 status))
        (is (= 2 (count (:queue body))))
        (is (= "identity" (:desc (first (:queue body)))))
        (is (= "anon" (:desc (last (:queue body)))))))))
