(ns drafter.auth.basic-test
  (:require [clojure.test :as t]
            [drafter.auth :as auth]
            [drafter.auth.basic :as sut]
            [drafter.user :as user]
            [drafter.user.memory-repository :as memory-repo]
            [drafter.auth.test-common :as auth-common]
            [drafter.util :as util])
  (:import clojure.lang.ExceptionInfo))

(defn- add-auth-header [m username password]
  (let [credentials (util/str->base64 (str username ":" password))]
    (assoc m "Authorization" (str "Basic " credentials))))

(defn- create-authorised-request [username password]
  {:uri "/test" :request-method :get :headers (add-auth-header {} username password)})

(defn- assert-is-unauthorised-basic-response [{:keys [status headers] :as response}]
  (t/is (= 401 status))
  (t/is (contains? headers "WWW-Authenticate")))

(t/deftest authenticate-user-test
  (let [username "test@example.com"
        password "dslkfjsejw"
        password-digest (user/get-digest password)
        user (user/create-user username :publisher password-digest)
        repo (memory-repo/init-repository* user)
        auth-method (sut/basic-auth-method repo)
        request (create-authorised-request username password)
        identity (auth-common/expect-authentication auth-method request)]
    (t/is (= (user/authenticated! user) identity))))

(t/deftest invalid-password-should-not-authenticate-test
  (let [username "test@example.com"
        user (user/create-user username :editor (user/get-digest "password"))
        repo (memory-repo/init-repository* user)
        auth-method (sut/basic-auth-method repo)
        request (create-authorised-request username "invalidpassword")
        ex (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request)))]
    (assert-is-unauthorised-basic-response (auth/authentication-failed-response ex))))

(t/deftest non-existent-user-should-not-authenticate-test
  (let [repo (memory-repo/init-repository*)
        auth-method (sut/basic-auth-method repo)
        request (create-authorised-request "missing@example.com" (user/get-digest "sdkfiwe"))
        ex (t/is (thrown? ExceptionInfo (auth-common/expect-authentication auth-method request)))]
    (assert-is-unauthorised-basic-response (auth/authentication-failed-response ex))))

(t/deftest should-not-parse-data-from-request-without-authentication
  (let [repo (memory-repo/init-repository*)
        auth-method (sut/basic-auth-method repo)
        request {:uri "/test" :request-method :get}]
    (t/is (nil? (auth/parse-request auth-method request)))))
