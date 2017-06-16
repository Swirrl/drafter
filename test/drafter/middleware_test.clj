(ns drafter.middleware-test
  (:require [buddy.auth :as auth]
            [buddy.core.codecs :refer [str->base64]]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [drafter
             [middleware :refer :all]
             [test-common :as tc]
             [user :as user]
             [user-test :refer [test-editor test-publisher]]]
            [drafter.rdf.sesame :as ses]
            [drafter.user.memory-repository :as memory-repo]
            [grafter.rdf
             [formats :as formats]
             [repository :as repo]]
            [ring.util.response :refer [response]])
  (:import clojure.lang.ExceptionInfo
           java.io.File
           (java.io ByteArrayInputStream)))

(defn- add-auth-header [m username password]
  (let [credentials (str->base64 (str username ":" password))]
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
        repo (memory-repo/create-repository* user)
        handler (basic-authentication repo identity)
        request (create-authorised-request username password)
        {:keys [identity] :as response} (handler request)]
    (is (auth/authenticated? response))
    (is (= (user/authenticated! user) identity))))

(deftest invalid-password-should-not-authenticate-test
  (let [username "test@example.com"
        user (user/create-user username :editor (user/get-digest "password"))
        repo (memory-repo/create-repository* user)
        handler (basic-authentication repo identity)
        request (create-authorised-request username "invalidpassword")
        response (handler request)]
    (is (= false (auth/authenticated? response)))))

(deftest non-existent-user-should-not-authenticate-test
  (let [repo (memory-repo/create-repository*)
        handler (basic-authentication repo identity)
        request (create-authorised-request "missing@example.com" (user/get-digest "sdkfiwe"))
        response (handler request)]
    (is (= false (auth/authenticated? response)))))

(deftest request-without-credentials-should-not-authenticate
  (let [repo (memory-repo/create-repository*)
        handler (basic-authentication repo identity)
        response (handler {:uri "/test" :request-method :get})]
    (is (= false (auth/authenticated? response)))))

(deftest handler-should-return-not-authenticated-response-if-inner-handler-throws-unauthorised
  (let [inner-handler (fn [req] (auth/throw-unauthorized {:message "Auth required"}))
        handler (basic-authentication (memory-repo/create-repository*) inner-handler)
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

(deftest required-rdf-content-type-test
  (testing "With valid RDF content type"
    (let [handler (fn [req] (:params req))
          wrapped-handler (require-rdf-content-type handler)
          content-type (.getDefaultMIMEType formats/rdf-nquads)
          request {:uri "/test" :headers {"content-type" content-type}}
          {:keys [rdf-format rdf-content-type]} (wrapped-handler request)]
      (is (= content-type rdf-content-type))
      (is (= formats/rdf-nquads rdf-format))))

  (testing "With unknown RDF content type"
    (let [handler (require-rdf-content-type ok-handler)
          request {:uri "/test" :headers {"content-type" "text/notrdf"}}
          response (handler request)]
      (tc/assert-is-unsupported-media-type-response response)))

  (testing "With no content type"
    (assert-unprocessable-with-no-content-type (require-rdf-content-type ok-handler)))

  (testing "With malformed content type"
    (assert-unprocessable-with-malformed-content-type (require-rdf-content-type ok-handler))))

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

(deftest sparql-query-parser-handler-test
  (let [handler (sparql-query-parser-handler identity)
        query-string "SELECT * WHERE { ?s ?p ?o }"]
    (testing "Valid GET request"
      (let [req {:request-method :get :query-params {:query query-string}}
            inner-req (handler req)]
        (is (= query-string (get-in inner-req [:sparql :query-string])))))

    (testing "Invalid GET request with missing query parameter"
      (let [resp (handler {:request-method :get :query-params {}})]
        (tc/assert-is-unprocessable-response resp)))

    (testing "Invalid GET request with multiple 'query' query parameters"
      (let [req {:request-method :get
                 :query-params {:query [query-string "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"]}}
            resp (handler req)]
        (tc/assert-is-unprocessable-response resp)))

    (testing "Valid form POST request"
      (let [req {:request-method :post
                 :headers {"content-type" "application/x-www-form-urlencoded"}
                 :form-params {"query" query-string}}
            inner-req (handler req)]
        (is (= query-string (get-in inner-req [:sparql :query-string])))))

    (testing "Invalid form POST request with missing query form parameter"
      (let [req {:request-method :post
                 :headers {"content-type" "application/x-www-form-urlencoded"}
                 :form-params {}}
            resp (handler req)]
        (tc/assert-is-unprocessable-response resp)))

    (testing "Invalid form POST request with multiple query form parameters"
      (let [req {:request-method :post
                 :headers {"content-type" "application/x-www-form-urlencoded"}
                 :form-params {"query" [query-string "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"]}}
            resp (handler req)]
        (tc/assert-is-unprocessable-response resp)))

    (testing "Valid body POST request"
      (let [req {:request-method :post
                 :headers {"content-type" "application/sparql-query"}
                 :body query-string}
            inner-req (handler req)]
        (is (= query-string (get-in inner-req [:sparql :query-string])))))

    (testing "Invalid body POST request missing body"
      (let [req {:request-method :post
                 :headers {"content-type" "application/sparql-query"}
                 :body nil}
            resp (handler req)]
        (tc/assert-is-unprocessable-response resp)))

    (testing "Invalid body POST request non-string body"
      (let [req {:request-method :post
                 :headers {"content-type" "application/sparql-query"}
                 :body (ByteArrayInputStream. (byte-array [1 2 3]))}
            resp (handler req)]
        (tc/assert-is-unprocessable-response resp)))

    (testing "POST request with invalid content type"
      (let [req {:request-method :post
                 :headers {"content-type" "text/plain"}
                 :params {:query query-string}}
            inner-req (handler req)]
        (is (= query-string (get-in inner-req [:sparql :query-string])))))

    (testing "Invalid request method"
      (let [req {:request-method :put :body query-string}
            resp (handler req)]
        (tc/assert-is-method-not-allowed-response resp)))))

(deftest sparql-prepare-query-handler-test
  (let [r (repo/repo)
        handler (sparql-prepare-query-handler r identity)]
    (testing "Valid query"
      (let [req (handler {:sparql {:query-string "SELECT * WHERE { ?s ?p ?o }"}})]
        (is (some? (get-in req [:sparql :prepared-query])))))

    (testing "Malformed SPARQL query"
      (let [response (handler {:sparql {:query-string "NOT A SPARQL QUERY"}})]
        (tc/assert-is-bad-request-response response)))))

(defn- prepare-query-str [query-str]
  (ses/prepare-query (repo/repo) query-str))

(deftest sparql-negotiation-handler-test
  (testing "Valid request"
    (let [handler (sparql-negotiation-handler identity)
          accept-content-type "application/n-triples"
          pquery (prepare-query-str "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
          request {:uri "/sparql"
                   :sparql {:prepared-query pquery}
                   :headers {"accept" accept-content-type}}
          {{:keys [format response-content-type]} :sparql} (handler request)]
      (is (= accept-content-type response-content-type))
      (is (some? format))))

  (testing "Content negotiation failure"
    (let [handler (sparql-negotiation-handler identity)
          pquery (prepare-query-str "SELECT * WHERE { ?s ?p ?o }")
          response (handler {:uri "/test"
                             :sparql {:prepared-query pquery}
                             :headers {"accept" "text/trig"}})]
      (tc/assert-is-not-acceptable-response response))))

(deftest sparql-timeout-handler-test
  (testing "With valid timeout"
    (let [timeout 30
          handler (sparql-timeout-handler (constantly timeout) identity)
          pquery (prepare-query-str "SELECT * WHERE { ?s ?p ?o }")
          request {:sparql {:prepared-query pquery}}]
      (handler request)
      (is (= timeout (.getMaxQueryTime pquery)))))

  (testing "With invalid timeout"
    (let [ex (IllegalArgumentException. "Invalid timeout")
          handler (sparql-timeout-handler (constantly ex) identity)
          pquery (prepare-query-str "SELECT * WHERE { ?s ?p ?o }")
          request {:sparql {:prepared-query pquery}}
          response (handler request)]
      (is (tc/assert-is-bad-request-response response)))))

(deftest negotiate-sparql-results-content-type-with-test
  (testing "Negotiation succeeds"
    (let [format formats/rdf-ntriples
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
