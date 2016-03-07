(ns drafter.middleware-test
  (:require [drafter.middleware :refer :all]
            [grafter.rdf.formats :as formats]
            [clojure.test :refer :all]
            [ring.util.response :refer [response]]
            [buddy.core.codecs :refer [str->base64]]
            [buddy.auth :as auth]
            [drafter.user :as user]
            [drafter.user.memory-repository :as memory-repo]
            [drafter.test-common :as tc])
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
        api-key-digest (user/get-digest api-key)
        user (user/create-user username :publisher api-key-digest)
        repo (memory-repo/create-repository* user)
        handler (basic-authentication repo "test" identity)
        request (create-authorised-request username api-key)
        {:keys [identity] :as response} (handler request)]
    (is (auth/authenticated? response))
    (is (= user identity))))

(deftest invalid-api-key-should-not-authenticate-test
  (let [username "test@example.com"
        user (user/create-user username :editor (user/get-digest "apikey"))
        repo (memory-repo/create-repository* user)
        handler (basic-authentication repo "test" identity)
        request (create-authorised-request username "invalidkey")
        response (handler request)]
    (is (= false (auth/authenticated? response)))))

(deftest non-existent-user-should-not-authenticate-test
  (let [repo (memory-repo/create-repository*)
        handler (basic-authentication repo "test" identity)
        request (create-authorised-request "missing@example.com" (user/get-digest "sdkfiwe"))
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

(deftest require-params-test
  (testing "Request with params"
    (let [invoked-inner (atom false)
          wrapped-handler (require-params #{:p1 :p2} (notifying-handler invoked-inner))
          request {:uri "/test" :request-method :get :params {:p1 "p1" :p2 "p2" :other "other"}}
          response (wrapped-handler request)]
      (is @invoked-inner)))

  (testing "Request with missing params"
    (let [invoked-inner (atom false)
          wrapped-handler (require-params #{:p1 :p2} (notifying-handler invoked-inner))
          request {:uri "/test" :request-method :get :params {:p2 "p2" :p3 "p3"}}
          response (wrapped-handler request)]
      (is (= false @invoked-inner))
      (tc/assert-is-unprocessable-response response))))

(deftest allowed-methods-handler-test
  (testing "Allowed method"
    (let [invoked-inner (atom false)
          wrapped-handler (allowed-methods-handler #{:get :post} (notifying-handler invoked-inner))
          request {:uri "/test" :request-method :get}
          response (wrapped-handler request)]
      (is @invoked-inner)))

  (testing "Disallowed method"
    (let [invoked-inner (atom false)
          wrapped-handler (allowed-methods-handler #{:get :post} (notifying-handler invoked-inner))
          request {:uri "/test" :request-method :delete}
          response (wrapped-handler request)]
      (is (= false @invoked-inner))
      (tc/assert-is-method-not-allowed-response response))))

(defn- ok-handler [request]
  (response "OK"))

(defn- assert-unprocessable-with-no-content-type [handler]
  (let [response (handler {:uri "/test"})]
    (tc/assert-is-unprocessable-response response)))

(defn- assert-unprocessable-with-malformed-content-type [handler]
  (let [response (handler {:uri "/test" :headers {"content-type" "malformed"}})]
    (tc/assert-is-unprocessable-response response)))

(deftest require-content-type-test
  (testing "With valid content type"
    (let [handler (require-content-type ok-handler)
          request {:uri "/test" :headers {"content-type" "text/plain"}}
          response (handler request)]
      (tc/assert-is-ok-response response)))

  (testing "With no content type"
    (assert-unprocessable-with-no-content-type (require-content-type ok-handler)))

  (testing "With malformed content type"
    (assert-unprocessable-with-malformed-content-type (require-content-type ok-handler))))

(deftest rdf-content-type-handler-test
  (testing "With valid RDF content type"
    (let [inner-handler (fn [req] (:params req))
          wrapped-handler (rdf-content-type-handler inner-handler)
          content-type (.getDefaultMIMEType formats/rdf-nquads)
          request {:uri "/test" :headers {"content-type" content-type}}
          {:keys [rdf-format rdf-content-type]} (wrapped-handler request)]
      (is (= content-type rdf-content-type))
      (is (= formats/rdf-nquads rdf-format))))

  (testing "With unknown RDF content type"
    (let [handler (rdf-content-type-handler ok-handler)
          request {:uri "/test" :headers {"content-type" "text/notrdf"}}
          response (handler request)]
      (tc/assert-is-unsupported-media-type-response response)))

  (testing "With no content type"
    (assert-unprocessable-with-no-content-type (rdf-content-type-handler ok-handler)))

  (testing "With malformed content type"
    (assert-unprocessable-with-malformed-content-type (rdf-content-type-handler ok-handler))))
