(ns drafter.middleware-test
  (:require [drafter.middleware :refer :all]
            [clojure.test :refer :all]
            [buddy.core.codecs :refer [str->base64]]
            [buddy.auth :as auth]
            [drafter.user :as user]
            [drafter.user.memory-repository :as memory-repo])
  (:import [clojure.lang ExceptionInfo]))

(defn- add-auth-header [m username password]
  (let [credentials (str->base64 (str username ":" password))]
    (assoc m "Authorization" (str "Basic " credentials))))

(defn- create-authorised-request [username password]
  {:uri "/test" :request-method :get :headers (add-auth-header {} username password)})

(defn- assert-is-unauthorised-basic-response [{:keys [status headers] :as response}]
  (is (= 401 status))
  (is (contains? headers "WWW-Authenticate")))

(deftest authenticate-user-test
  (let [username "test@example.com"
        api-key "dslkfjsejw"
        user (user/create-user username :publisher api-key)
        repo (memory-repo/create-repository* user)
        handler (basic-authentication repo "test" identity)
        request (create-authorised-request username api-key)
        {:keys [identity] :as response} (handler request)]
    (is (auth/authenticated? response))
    (is (= user identity))))

(deftest invalid-api-key-should-not-authenticate-test
  (let [username "test@example.com"
        user (user/create-user username :editor "apikey")
        repo (memory-repo/create-repository* user)
        handler (basic-authentication repo "test" identity)
        request (create-authorised-request username "invalidkey")
        response (handler request)]
    (is (= false (auth/authenticated? response)))))

(deftest non-existent-user-should-not-authenticate-test
  (let [repo (memory-repo/create-repository*)
        handler (basic-authentication repo "test" identity)
        request (create-authorised-request "missing@example.com" "sdkfiwe")
        response (handler request)]
    (is (= false (auth/authenticated? response)))))

(deftest request-without-credentials-should-not-authenticate
  (let [repo (memory-repo/create-repository*)
        handler (basic-authentication repo "test" identity)
        response (handler {:uri "/test" :request-method :get})]
    (is (= false (auth/authenticated? response)))))

(deftest handler-should-return-not-authenticated-response-if-inner-handler-throws-unauthorised
  (let [inner-handler (fn [req] (auth/throw-unauthorized {:message "Auth required"}))
        handler (basic-authentication (memory-repo/create-repository*) "test" inner-handler)
        response (handler {:uri "/test" :request-method :get})]
    (assert-is-unauthorised-basic-response response)))

(deftest require-authenticated-should-call-inner-handler-if-authenticated
  (let [handler (require-authenticated identity)]
    (is (thrown? ExceptionInfo (handler {:uri "/foo" :request-method :get})))))

(deftest required-authenticated-should-throw-if-unauthenticated
  (let [user (user/create-user "test@example.com" :publisher "sdlkf")
        request {:uri "/foo" :request-method :post :identity user}
        inner-was-called (atom false)
        inner-handler (fn [r] (reset! inner-was-called true) r)
        handler (require-authenticated inner-handler)]
    (handler request)
    (is (= true @inner-was-called))))
