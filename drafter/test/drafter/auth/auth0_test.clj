(ns drafter.auth.auth0-test
  (:require [clojure.test :as t]
            [drafter.auth :as auth]
            [drafter.auth.test-common :as auth-common]
            [drafter.auth.auth0 :as sut]
            [drafter.test-common :as tc]
            [drafter.user :as user])
  (:import clojure.lang.ExceptionInfo))

(t/deftest read-role-test
  (t/testing "Valid role"
    (t/is (= :publisher (sut/read-role "drafter:publisher"))))

  (t/testing "Non-drafter role"
    (t/is (nil? (sut/read-role "muttnik:editor"))))

  (t/testing "Invalid drafter role"
    (t/is (nil? (sut/read-role "drafter:invalid"))))

  (t/testing "No prefix"
    (t/is (nil? (sut/read-role "manager")))))

(defn- get-auth-method [system]
  (let [jwk (:drafter.auth.auth0/mock-jwk system)
        auth0-client (:swirrl.auth0/client system)]
    (sut/auth0-auth-method auth0-client jwk)))

(defn- add-auth-header [request access-token]
  (assoc-in request [:headers "Authorization"] (str "Bearer " access-token)))

(t/deftest should-not-parse-data-from-request-with-no-auth
  (tc/with-system
    [:drafter.auth.auth0/mock-jwk :swirrl.auth0/client]
    [system "test-system.edn"]
    (let [auth-method (get-auth-method system)
          request {:uri "/test" :request-method :get}]
      (t/is (nil? (auth/parse-request auth-method request))))))

(t/deftest should-not-parse-data-from-request-with-other-auth
  (tc/with-system
    [:drafter.auth.auth0/mock-jwk :swirrl.auth0/client]
    [system "test-system.edn"]
    (let [auth-method (get-auth-method system)
          request {:uri "/test" :request-method :get :headers {"Authorization" "Token notabearertoken"}}]
      (t/is (nil? (auth/parse-request auth-method request))))))

(t/deftest should-authenticate-user
  (tc/with-system
    [:drafter.auth.auth0/mock-jwk :swirrl.auth0/client]
    [system "test-system.edn"]
    (let [auth-method (get-auth-method system)
          username "test@example.com"
          token (tc/user-access-token username "drafter:editor")
          request (add-auth-header {:uri "/test" :request-method :get} token)
          user (auth-common/expect-authentication auth-method request)
          expected-user (user/create-authenticated-user username :editor)]
      (t/is (= expected-user user)))))

(t/deftest should-reject-invalid-token
  (tc/with-system
    [:drafter.auth.auth0/mock-jwk :swirrl.auth0/client]
    [system "test-system.edn"]
    (let [auth-method (get-auth-method system)
          token "notabearertoken"
          request (add-auth-header {:uri "/test" :request-method :get} token)]
      (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request))))))
