(ns drafter.handler-test
  (:require
   [drafter.user-test :refer [test-norole test-reader test-editor]]
   [clojure.test :refer [deftest is]]
   [drafter.routes.sparql-test :refer [live-query]]
   [drafter.test-common :as tc])
  (:import java.util.UUID))

(deftest global-auth-test
  (let [root {:scheme :http :request-method :get :uri "/"}
        list (assoc root :uri "/v1/draftsets")]
    (tc/with-system [{handler :drafter.handler/app :as system}
                     "test-system.edn"]
      (do
        ;; On a default system without global auth, routes are publicly accessable
        (is (= 200 (:status (handler root))))
        (is (= 200 (:status (handler (tc/with-identity test-norole root)))))
        (is (= 200 (:status (handler (tc/with-identity test-reader root)))))
        (is (= 200 (:status (handler (tc/with-identity test-editor root)))))
        ;; unless explicity configured otherwise
        (is (= 401 (:status (handler list))))
        (is (= 403 (:status (handler (tc/with-identity test-norole list)))))
        (is (= 403 (:status (handler (tc/with-identity test-reader list)))))
        (is (= 200 (:status (handler (tc/with-identity test-editor list)))))))
    (tc/with-system [{handler :drafter.handler/app :as system}
                     "global-auth-test-system.edn"]
      (do
        ;; On a system with global auth, reader role or stronger are required
        ;; for all routes
        (is (= 401 (:status (handler root))))
        (is (= 403 (:status (handler (tc/with-identity test-norole root)))))
        (is (= 200 (:status (handler (tc/with-identity test-reader root)))))
        (is (= 200 (:status (handler (tc/with-identity test-editor root)))))
        ;; but reader isn't strong enough for editor routes
        (is (= 401 (:status (handler list))))
        (is (= 403 (:status (handler (tc/with-identity test-norole list)))))
        (is (= 403 (:status (handler (tc/with-identity test-reader list)))))
        (is (= 200 (:status (handler (tc/with-identity test-editor list)))))))))
