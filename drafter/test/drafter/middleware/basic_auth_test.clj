(ns ^:basic-auth drafter.middleware.basic-auth-test
  (:require [buddy.auth :as auth]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [drafter.middleware :refer :all]
            [drafter.middleware.auth :refer :all]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user.memory-repository :as memory-repo]
            [drafter.util :as util]
            [grafter-2.rdf4j.formats :as formats]
            [ring.util.response :refer [response]])
  (:import clojure.lang.ExceptionInfo
           [java.io ByteArrayInputStream File]
           org.eclipse.rdf4j.rio.RDFFormat))

(use-fixtures :each tc/with-spec-instrumentation)

(defn- add-auth-header [m username password]
  (let [credentials (util/str->base64 (str username ":" password))]
    (assoc m "Authorization" (str "Basic " credentials))))

(defn- create-authorised-request [username password]
  {:uri "/test" :request-method :get :headers (add-auth-header {} username password)})

(defn- assert-is-unauthorised-basic-response [{:keys [status headers] :as response}]
  (is (= 401 status))
  (is (contains? headers "WWW-Authenticate")))

(defn basic-authentication [repo handler]
  (let [wrapper (make-authenticated-wrapper repo {})]
    (wrapper handler)))

(deftest authenticate-user-test
  (let [username "test@example.com"
        password "dslkfjsejw"
        password-digest (user/get-digest password)
        user (user/create-user username :publisher password-digest)
        repo (memory-repo/init-repository* user)
        handler (basic-authentication repo identity)
        request (create-authorised-request username password)
        {:keys [identity] :as response} (handler request)]
    (is (auth/authenticated? response))
    (is (= (user/authenticated! user) identity))))

(deftest invalid-password-should-not-authenticate-test
  (let [username "test@example.com"
        user (user/create-user username :editor (user/get-digest "password"))
        repo (memory-repo/init-repository* user)
        handler (basic-authentication repo identity)
        request (create-authorised-request username "invalidpassword")
        response (handler request)]
    (is (= false (auth/authenticated? response)))))

(deftest non-existent-user-should-not-authenticate-test
  (let [repo (memory-repo/init-repository*)
        handler (basic-authentication repo identity)
        request (create-authorised-request "missing@example.com" (user/get-digest "sdkfiwe"))
        response (handler request)]
    (is (= false (auth/authenticated? response)))))

(deftest request-without-credentials-should-not-authenticate
  (let [repo (memory-repo/init-repository*)
        handler (basic-authentication repo identity)
        response (handler {:uri "/test" :request-method :get})]
    (is (= false (auth/authenticated? response)))))

(deftest handler-should-return-not-authenticated-response-if-inner-handler-throws-unauthorised
  (let [inner-handler (fn [req] (auth/throw-unauthorized {:message "Auth required"}))
        handler (basic-authentication (memory-repo/init-repository*) inner-handler)
        response (handler {:uri "/test" :request-method :get})]
    (assert-is-unauthorised-basic-response response)))

(deftest require-authenticated-should-call-inner-handler-if-authenticated
  (let [handler (require-authenticated identity)]
    (is (thrown? ExceptionInfo (handler {:uri "/foo" :request-method :get})))))

(defn- notifying-handler [a]
  (fn [r]
    (reset! a true)
    (response "")))

(deftest required-authenticated-should-throw-if-unauthenticated
  (let [user (user/create-user "test@example.com" :publisher "sdlkf")
        request {:uri "/foo" :request-method :post :identity user}
        invoked-inner (atom false)
        handler (require-authenticated (notifying-handler invoked-inner))]
    (handler request)
    (is (= true @invoked-inner))))
