(ns drafter.middleware-test
  (:require [buddy.auth :as auth]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [drafter.middleware :refer :all]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user.memory-repository :as memory-repo]
            [drafter.util :as util]
            [environ.core :refer [env]]
            [grafter-2.rdf4j.formats :as formats]
            [ring.util.response :refer [response]]
            [cheshire.core :as json]
            [clj-time.coerce :refer [to-date]]
            [clj-time.core :as time]
            [integrant.core :as ig]
            )
  (:import clojure.lang.ExceptionInfo
           [java.io ByteArrayInputStream File]
           org.eclipse.rdf4j.rio.RDFFormat
           com.auth0.jwt.algorithms.Algorithm
           [com.auth0.jwt.exceptions InvalidClaimException
            JWTVerificationException TokenExpiredException]
           [com.auth0.jwk Jwk JwkProvider]
           com.auth0.jwt.JWT
           java.security.KeyPairGenerator
           java.util.Base64
           ))

(use-fixtures :each tc/with-spec-instrumentation)

(defn- add-auth-header [m access-token]
  (assoc m "Authorization" (str "Bearer " access-token)))

(defn- create-authorised-request [access-token]
  {:uri "/test" :request-method :get :headers (add-auth-header {} access-token)})

(defn- assert-is-unauthorised-response [{:keys [status headers] :as response}]
  (is (= 401 status))
  ; (is (contains? headers "WWW-Authenticate"))
  )

(tc/deftest-system-with-keys authenticate-user-test
  [:drafter.middleware/wrap-auth]
  [{:keys [:drafter.middleware/wrap-auth]} "test-system.edn"]
  (let [username "test@example.com"
        user {:email username :role :pmd.role/drafter:publisher}
        token (tc/user-access-token username "drafter:publisher")
        request (create-authorised-request token)
        handler (wrap-auth identity)
        {:keys [identity] :as response} (handler request)]
    (is (auth/authenticated? response))
    (is (= (user/authenticated! user) identity))))

(tc/deftest-system-with-keys invalid-token-should-not-authenticate-test
  [:drafter.middleware/wrap-auth]
  [{:keys [:drafter.middleware/wrap-auth]} "test-system.edn"]
  (let [username "test@example.com"
        user {:email username :role :pmd.role/drafter:editor}
        token "blahblahblah"
        request (create-authorised-request token)
        handler (wrap-auth identity)
        response (handler request)]
    (is (= false (auth/authenticated? response)))))

;; deftest non-existent-user-should-not-authenticate-test
;; TODO: ^^ test doesn't make sense in light of Auth0

(tc/deftest-system-with-keys request-without-credentials-should-not-authenticate
  [:drafter.middleware/wrap-auth]
  [{:keys [:drafter.middleware/wrap-auth]} "test-system.edn"]
  (let [handler (wrap-auth identity)
        response (handler {:uri "/test" :request-method :get})]
    (is (= false (auth/authenticated? response)))))

(tc/deftest-system-with-keys handler-should-return-not-authenticated-response-if-inner-handler-returns-unauthorised
  [:drafter.middleware/wrap-auth]
  [{:keys [:drafter.middleware/wrap-auth]} "test-system.edn"]
  (let [inner-handler (fn [req] {:status 401 :body "Auth required"})
        handler (wrap-auth inner-handler)
        response (handler {:uri "/test" :request-method :get})]
    (assert-is-unauthorised-response response)))

;; (deftest require-authenticated-should-call-inner-handler-if-authenticated
;;   (let [handler (require-authenticated identity)]
;;     (is (thrown? ExceptionInfo (handler {:uri "/foo" :request-method :get})))))
;; TODO: what about this? need the logging/stats from require-authenticated, but
;; not the other stuff

(defn- notifying-handler [a]
  (fn [r]
    (reset! a true)
    (response "")))

;; (deftest required-authenticated-should-throw-if-unauthenticated
;;   (let [user (user/create-user "test@example.com" :publisher "sdlkf")
;;         request {:uri "/foo" :request-method :post :identity user}
;;         invoked-inner (atom false)
;;         handler (require-authenticated (notifying-handler invoked-inner))]
;;     (handler request)
;;     (is (= true @invoked-inner))))

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
      (let [req {:request-method :get :query-params {"query" query-string}}
            inner-req (handler req)]
        (is (= query-string (get-in inner-req [:sparql :query-string])))))

    (testing "Invalid GET request with missing query parameter"
      (let [resp (handler {:request-method :get :query-params {}})]
        (tc/assert-is-unprocessable-response resp)))

    (testing "Invalid GET request with multiple 'query' query parameters"
      (let [req {:request-method :get
                 :query-params {"query" [query-string "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"]}}
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
