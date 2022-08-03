(ns drafter.middleware-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all :as t]
            [drafter.middleware :refer :all :as sut]
            [drafter.auth :as auth]
            [drafter.test-common :as tc]
            [grafter-2.rdf4j.formats :as formats]
            [ring.util.response :refer [response]]
            [drafter.responses :as response]
            [drafter.user :as user])
  (:import [java.io File]
           org.eclipse.rdf4j.rio.RDFFormat
           [java.util.zip GZIPOutputStream]
           [clojure.lang ExceptionInfo]))

(use-fixtures :each tc/with-spec-instrumentation)

(defn- notifying-handler [a]
  (fn [r]
    (reset! a true)
    (response "")))

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

(deftest optional-enum-param-test
  (let [param-name :p
        inner-handler (fn [request] (get-in request [:params param-name]))
        allowed-values #{:foo :bar :quux}
        default-value :bar
        handler (optional-enum-param param-name allowed-values default-value inner-handler)]

    (testing "Missing value"
      (is (= default-value (handler {}))))

    (testing "Valid value"
      (let [expected-value (second allowed-values)
            request {:params {param-name (name expected-value)}}]
        (is (= expected-value (handler request)))))

    (testing "Invalid value"
      (let [request {:params {param-name "invalid-value"}}
            response (handler request)]
        (tc/assert-is-unprocessable-response response)))))

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

(deftest required-rdf-content-type-test
  (testing "With valid RDF content type"
    (let [handler (fn [req] (:params req))
          wrapped-handler (require-rdf-content-type handler)
          content-type (.getDefaultMIMEType (formats/->rdf-format :nq))
          request {:uri "/test" :headers {"content-type" content-type}}
          {:keys [rdf-format rdf-content-type]} (wrapped-handler request)]
      (is (= content-type rdf-content-type))
      (is (= RDFFormat/NQUADS rdf-format))))

  (testing "With unknown RDF content type"
    (let [handler (require-rdf-content-type ok-handler)
          request {:uri "/test" :headers {"content-type" "text/notrdf"}}
          response (handler request)]
      (tc/assert-is-unsupported-media-type-response response)))

  (testing "With no content type"
    (assert-unprocessable-with-no-content-type (require-rdf-content-type ok-handler)))

  (testing "With malformed content type"
    (assert-unprocessable-with-malformed-content-type (require-rdf-content-type ok-handler))))

(defn- inflates-gzipped? [encoding]
  (tc/with-temp-file "drafter-test" ".txt.gz"
    temp-file
    (let [uncompressed-body "The quick brown fox jumped over the lazy dog"
          test-handler (inflate-gzipped (fn [request]
                                          (with-open [is (:body request)]
                                            (slurp is))))
          req {:headers {"content-type" "text/plain"
                         "content-encoding" encoding}
               :body    temp-file}]
      (with-open [os (GZIPOutputStream. (io/output-stream temp-file))]
        (spit os uncompressed-body))
      (= uncompressed-body (test-handler req)))))

(defn- keeps-ungzipped? [headers]
  (tc/with-temp-file "drafter-test" ".txt"
    temp-file
    (let [uncompressed-body "The quick brown fox jumped over the lazy dog"
          test-handler (inflate-gzipped (fn [request] (:body request)))
          request {:headers headers :body temp-file}]
      (spit temp-file uncompressed-body)
      (= temp-file (test-handler request)))))

(deftest inflate-gzipped-test
  (testing "Compressed"
    (are [content-encoding] (= true (inflates-gzipped? content-encoding))
      "gzip"
      "x-gzip"
      "GZip"
      "X-GZIP"))

  (testing "Uncompressed"
    (are [headers] (= true (keeps-ungzipped? headers))
      {}
      {"content-type" "text/plain"
       "content-encoding" "compress"})))

(deftest temp-file-body-test
  (testing "Creates file"
    (let [inner-handler (fn [{:keys [body]}] (instance? File body))
          handler (temp-file-body inner-handler)
          body-stream (tc/string->input-stream "body contents")
          result (handler {:uri "/test" :request-method :get :body body-stream})]
      (is result)))

  (testing "Copies body contents"
    (let [inner-handler (fn [{:keys [body]}]
                          (first (line-seq (io/reader body))))
          handler (temp-file-body inner-handler)
          body-text "The quick brown fox jumped"
          body-stream (tc/string->input-stream body-text)
          result (handler {:uri "/test" :request-method :post :body body-stream})]
      (is (= body-text result)))))

(deftest negotiate-sparql-results-content-type-with-test
  (testing "Negotiation succeeds"
    (let [format RDFFormat/NTRIPLES
          response-content-type "text/plain"
          handler (negotiate-sparql-results-content-type-with (constantly [format response-content-type]) ":(" identity)
          request {:headers {"accept" "text/plain"}}
          inner-request (handler request)]
      (is (= format (get-in inner-request [:sparql :format])))
      (is (= response-content-type (get-in inner-request [:sparql :response-content-type])))))

  (testing "Negotiation fails"
    (let [handler (negotiate-sparql-results-content-type-with (constantly nil) ":(" identity)
          request {:uri "/test"
                   :request-method :get
                   :headers {"accept" "text/plain"}}
          response (handler request)]
      (tc/assert-is-not-acceptable-response response))))

(def ^{:doc "Authentication method that never matches a request"} never-matches-auth-method
  (reify auth/AuthenticationMethod
    (parse-request [_this _request] nil)
    (authenticate [_this _request _state]
      ;; NOTE: should never be called
      (throw (ex-info "Authentication failed" {})))))

(defn- fails-with-auth-method
  "Returns an authentication method that always matches a request
   and fails authentication with the given exception"
  [ex]
  (reify auth/AuthenticationMethod
    (parse-request [_this _request] {:dummy :state})
    (authenticate [_this _request _state]
      (throw ex))))

(def always-fails-auth-method
  (fails-with-auth-method (ex-info "Authentication failed" {})))

(defn- succeeds-with-auth-method
  "Returns an authentication method that always matches a request and
   successfully authenticates with the given user"
  [user]
  (reify auth/AuthenticationMethod
    (parse-request [_this _request] {:dummy :state})
    (authenticate [_this _request _state]
      user)))

(t/deftest authenticate-request-test
  (t/testing "No matching auth method"
    (let [auth-methods [never-matches-auth-method]
          request {:uri "/test" :request-method :get}]
      (t/is (nil? (sut/authenticate-request auth-methods request)))))

  (t/testing "Authentication succeeds"
    (let [user (user/create-authenticated-user "test@example.com" #{:drafter:draft:view})
          auth-methods [(succeeds-with-auth-method user)]
          request {:uri "/test" :request-method :get}]
      (t/is (= user (sut/authenticate-request auth-methods request)))))

  (t/testing "Authenticates with first matching handler"
    (let [user1 (user/create-authenticated-user "test1@example.com" #{:drafter:draft:view})
          user2 (user/create-authenticated-user "test2@example.com" #{:drafter:draft:view})
          auth-methods [(succeeds-with-auth-method user1)
                        (succeeds-with-auth-method user2)]
          request {:uri "/test" :request-method :get}]
      (t/is (= user1 (authenticate-request auth-methods request)))))

  (t/testing "Authentication fails"
    (let [auth-methods [always-fails-auth-method]
          request {:uri "/test" :request-method :get}]
      (t/is (thrown? ExceptionInfo (authenticate-request auth-methods request))))))

(t/deftest wrap-authenticate-test
  (t/testing "Allows OPTIONS requests"
    (let [auth-methods [always-fails-auth-method]
          handler (sut/wrap-authenticate identity auth-methods)
          request {:uri "/test" :request-method :options}
          response (handler request)]
      (t/is (= request response))))

  (t/testing "Allows swagger UI routes"
    (let [auth-methods [always-fails-auth-method]
          handler (sut/wrap-authenticate identity auth-methods)
          request {:uri "/swagger/swagger.json" :request-method :get}
          response (handler request)]
      (t/is (= request response))))

  (t/testing "Allows requests with existing identity"
    (let [auth-methods [always-fails-auth-method]
          handler (sut/wrap-authenticate identity auth-methods)
          user (user/create-authenticated-user "test@example.com" #{:drafter:draft:view})
          request {:uri "/test" :request-method :get :identity user}
          response (handler request)]
      (t/is (= request response))))

  (t/testing "Authenticates user"
    (let [user (user/create-authenticated-user "test@example.com" #{:drafter:draft:view})
          auth-methods [(succeeds-with-auth-method user)]
          handler (sut/wrap-authenticate identity auth-methods)
          request {:uri "/test" :request-method :get}
          response (handler request)]
      (t/is (= user (:identity response)))))

  (t/testing "Not authenticated if no auth methods match"
    (let [auth-methods [never-matches-auth-method]
          handler (sut/wrap-authenticate identity auth-methods)
          request {:uri "/test" :request-method :get}
          response (handler request)]
      (tc/assert-is-unauthorised-response response)))

  (t/testing "Not authenticated if authentication fails"
    (let [auth-methods [always-fails-auth-method]
          handler (sut/wrap-authenticate identity auth-methods)
          request {:uri "/test" :request-method :get}
          response (handler request)]
      (tc/assert-is-unauthorised-response response)))

  (t/testing "Custom response from authentication error"
    (let [auth-ex (ex-info "Authentication failed" {:response (response/forbidden-response "Forbidden")})
          auth-methods [(fails-with-auth-method auth-ex)]
          handler (sut/wrap-authenticate identity auth-methods)
          request {:uri "/test" :request-method :get}
          response (handler request)]
      (tc/assert-is-forbidden-response response))))
