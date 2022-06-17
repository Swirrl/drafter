(ns drafter.auth.jws-test
  (:require [clojure.test :as t]
            [drafter.auth :as auth]
            [drafter.auth.jws :as sut]
            [drafter.auth.test-common :as auth-common]
            [cheshire.core :as json]
            [buddy.sign.jws :as jws])
  (:import [clojure.lang ExceptionInfo]))

(defn- add-token-header [request token]
  (assoc-in request [:headers "authorization"] (str "Token " token)))

(defn- sign-doc [doc signing-key]
  (let [doc-str (json/generate-string doc)]
    (jws/sign doc-str signing-key :alg :hs256)))

(defn- get-claims [user]
  (merge user {:iss "publishmydata" :aud "drafter"}))

(defn- create-token [user signing-key]
  (sign-doc (get-claims user) signing-key))

(defn- add-user [request user signing-key]
  (add-token-header request (create-token user signing-key)))

(t/deftest should-not-parse-data-from-request-without-authentication
  (let [auth-method (sut/jws-auth-method "test")
        request {:uri "/test" :request-method :get}]
    (t/is (nil? (auth/parse-request auth-method request)))))

(t/deftest should-authenticate-request-with-valid-token
  (let [user {:email "test@example.com" :role :publisher}
        signing-key "test"
        auth-method (sut/jws-auth-method signing-key)
        request (add-user {:uri "/test" :request-method :get} user signing-key)
        identity (auth-common/expect-authentication auth-method request)]
    (t/is (= user identity))))

(t/deftest should-reject-token-with-invalid-issuer
  (let [doc {:user "test@example.com"
             :role :editor
             :iss "notpmd"
             :aud "drafter"}
        signing-key "test"
        token (sign-doc doc signing-key)
        auth-method (sut/jws-auth-method signing-key)
        request (add-token-header {:uri "/test" :request-method :get} token)]
    (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request)))))

(t/deftest should-reject-token-with-invalid-audience
  (let [doc {:user "test@example.com"
             :role :manager
             :iss "publishmydata"
             :aud "notdrafter"}
        signing-key "test"
        token (sign-doc doc signing-key)
        auth-method (sut/jws-auth-method signing-key)
        request (add-token-header {:uri "/test" :request-method :get} token)]
    (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request)))))

(t/deftest should-reject-token-with-invalid-signing-algorithm
  (let [user {:email "test@example.com" :role :manager}
        claims (get-claims user)
        doc-str (json/generate-string claims)
        signing-key "test"
        token (jws/sign doc-str signing-key {:alg :hs512})
        request (add-token-header {:uri "/test" :request-method :get} token)
        auth-method (sut/jws-auth-method signing-key)]
    (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request)))))

(t/deftest should-reject-token-with-invalid-email-address
  (let [user {:email "not a valid email" :role :publisher}
        signing-key "test"
        request (add-user {:uri "/test" :request-method :get} user signing-key)
        auth-method (sut/jws-auth-method signing-key)]
    (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request)))))

(t/deftest should-reject-token-with-no-email-address
  (let [user {:role :editor}
        signing-key "test"
        request (add-user {:uri "/test" :request-method :get} user signing-key)
        auth-method (sut/jws-auth-method signing-key)]
    (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request)))))

(t/deftest should-reject-token-with-no-role
  (let [user {:email "test@example.com"}
        signing-key "test"
        request (add-user {:uri "/test" :request-method :get} user signing-key)
        auth-method (sut/jws-auth-method signing-key)]
    (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request)))))

(t/deftest should-reject-token-with-invalid-role
  (let [user {:email "test@example.com" :role :ubermensch}
        signing-key "test"
        request (add-user {:uri "/test" :request-method :get} user signing-key)
        auth-method (sut/jws-auth-method signing-key)]
    (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request)))))

(t/deftest should-reject-token-signed-with-different-key
  (let [user {:email "test@example.com" :role :editor}
        request (add-user {:uri "/test" :request-method :get} user "signing-key")
        auth-method (sut/jws-auth-method "verification-key")]
    (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request)))))
