(ns ^:auth0 drafter.middleware.auth0-auth-test
  (:require [buddy.auth :as auth]
            [clojure.java.io :as io]
            [clojure.test :refer :all :as t]
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
            [integrant.core :as ig])
  (:import clojure.lang.ExceptionInfo
           [java.io ByteArrayInputStream File]
           org.eclipse.rdf4j.rio.RDFFormat
           com.auth0.jwt.algorithms.Algorithm
           [com.auth0.jwt.exceptions InvalidClaimException
            JWTVerificationException TokenExpiredException]
           [com.auth0.jwk Jwk JwkProvider]
           com.auth0.jwt.JWT
           java.security.KeyPairGenerator
           java.util.Base64))

(use-fixtures :each tc/with-spec-instrumentation)

(def system "test-system.edn")

(defn- add-auth-header [m access-token]
  (assoc m "Authorization" (str "Bearer " access-token)))

(defn- create-authorised-request [access-token]
  {:uri "/test" :request-method :get :headers (add-auth-header {} access-token)})

(defn- assert-is-unauthorised-response [{:keys [status headers] :as response}]
  (is (= 401 status)))

(t/deftest authenticate-user-test
  (tc/with-system
    [:drafter.middleware/wrap-authenticate]
    [{:keys [:drafter.middleware/wrap-authenticate]} system]
    (let [username "test@example.com"
          permissions #{:drafter:cat:pet :drafter:missiles:launch}
          user {:email username :role :publisher :permissions permissions}
          token (tc/user-access-token username "drafter:publisher" permissions)
          request (create-authorised-request token)
          handler (wrap-authenticate identity)
          {:keys [identity] :as response} (handler request)]
      (t/is (auth/authenticated? response))
      (t/is (= (user/authenticated! user) identity)))))

(tc/deftest-system-with-keys invalid-token-should-not-authenticate-test
  [:drafter.middleware/wrap-authenticate]
  [{:keys [:drafter.middleware/wrap-authenticate]} system]
  (let [username "test@example.com"
        user {:email username :role :pmd.role/drafter:editor}
        token "blahblahblah"
        request (create-authorised-request token)
        handler (wrap-authenticate identity)
        response (handler request)]
    (is (= false (auth/authenticated? response)))))

(tc/deftest-system-with-keys request-without-credentials-should-not-authenticate
  [:drafter.middleware/wrap-authenticate]
  [{:keys [:drafter.middleware/wrap-authenticate]} system]
  (let [handler (wrap-authenticate identity)
        response (handler {:uri "/test" :request-method :get})]
    (is (= false (auth/authenticated? response)))))

(tc/deftest-system-with-keys handler-should-return-not-authenticated-response-if-inner-handler-returns-unauthorised
  [:drafter.middleware/wrap-authenticate]
  [{:keys [:drafter.middleware/wrap-authenticate]} system]
  (let [inner-handler (fn [req] {:status 401 :body "Auth required"})
        handler (wrap-authenticate inner-handler)
        response (handler {:uri "/test" :request-method :get})]
    (assert-is-unauthorised-response response)))
