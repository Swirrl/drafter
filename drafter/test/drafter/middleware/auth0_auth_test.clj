(ns ^:auth0 drafter.middleware.auth0-auth-test
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

(tc/deftest-system-with-keys authenticate-user-test
  [:drafter.middleware/wrap-auth]
  [{:keys [:drafter.middleware/wrap-auth]} system]
  (let [username "test@example.com"
        user {:email username :role :publisher}
        token (tc/user-access-token username "drafter:publisher")
        request (create-authorised-request token)
        handler (wrap-auth identity)
        {:keys [identity] :as response} (handler request)]
    (is (auth/authenticated? response))
    (is (= (user/authenticated! user) identity))))

(tc/deftest-system-with-keys invalid-token-should-not-authenticate-test
  [:drafter.middleware/wrap-auth]
  [{:keys [:drafter.middleware/wrap-auth]} system]
  (let [username "test@example.com"
        user {:email username :role :pmd.role/drafter:editor}
        token "blahblahblah"
        request (create-authorised-request token)
        handler (wrap-auth identity)
        response (handler request)]
    (is (= false (auth/authenticated? response)))))

(tc/deftest-system-with-keys request-without-credentials-should-not-authenticate
  [:drafter.middleware/wrap-auth]
  [{:keys [:drafter.middleware/wrap-auth]} system]
  (let [handler (wrap-auth identity)
        response (handler {:uri "/test" :request-method :get})]
    (is (= false (auth/authenticated? response)))))

(tc/deftest-system-with-keys handler-should-return-not-authenticated-response-if-inner-handler-returns-unauthorised
  [:drafter.middleware/wrap-auth]
  [{:keys [:drafter.middleware/wrap-auth]} system]
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
