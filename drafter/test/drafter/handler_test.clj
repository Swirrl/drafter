(ns ^:rest-api drafter.handler-test
  (:require
   [drafter.user-test :refer [test-norole test-reader test-editor test-publisher]]
   [clojure.test :refer [deftest are]]
   [drafter.routes.sparql-test :refer [live-query]]
   [drafter.test-common :as tc])
  (:import java.util.UUID))

(deftest global-auth-test
  (let [root {:scheme :http :request-method :get :uri "/"}
        list (assoc root :uri "/v1/draftsets")]
    (tc/with-system [{handler :drafter.handler/app :as system}
                     "test-system.edn"]
      (are [status req] (= status (:status (handler req)))
        ;; On a default system without global auth, routes are publicly
        ;; accessable by default
        200 root
        200 (tc/with-identity test-norole root)
        200 (tc/with-identity test-reader root)
        200 (tc/with-identity test-editor root)
        200 (tc/with-identity test-publisher root)

        ;; The list draftset route requires editor role
        401 list
        403 (tc/with-identity test-norole list)
        403 (tc/with-identity test-reader list)
        200 (tc/with-identity test-editor list)
        ;; Publisher implies editor
        200 (tc/with-identity test-publisher list)))
    (tc/with-system [{handler :drafter.handler/app :as system}
                     "global-auth-test-system.edn"]
      (are [status req] (= status (:status (handler req)))
        ;; On a system with :drafter/global-auth? true, reader role or stronger
        ;; are required for all routes
        401 root
        403 (tc/with-identity test-norole root)
        200 (tc/with-identity test-reader root)
        200 (tc/with-identity test-editor root)
        200 (tc/with-identity test-publisher root)
        ;; But list still requires editor role
        401 list
        403 (tc/with-identity test-norole list)
        403 (tc/with-identity test-reader list)
        200 (tc/with-identity test-editor list)
        200 (tc/with-identity test-publisher list)))))
