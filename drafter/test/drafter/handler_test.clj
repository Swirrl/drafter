(ns ^:rest-api drafter.handler-test
  (:require
   [clojure.test :refer [deftest are]]
   [drafter.feature.endpoint.public :as public]
   [drafter.routes.sparql-test :refer [live-query]]
   [drafter.test-common :as tc]
   [drafter.user-test :refer [test-norole test-access test-editor test-publisher]])
  (:import java.util.UUID))

(deftest global-auth-test
  (let [get {:scheme :http :request-method :get}
        whitelisted (assoc get :uri "/swagger/swagger.json")
        get-public (assoc
                    get
                    :uri "/v1/sparql/live"
                    :headers {"accept" "text/plain"}
                    :query-string
                    "query=select%20%2A%20where%20%7B%3Fs%20%3Fp%20%3Fo%7D")
        options-public (assoc get-public
                              :request-method :options
                              :headers {"origin" "http://swirrl.com"
                                        "access-control-request-method" "post"
                                        "access-control-request-headers" "accept"})
        list-draftsets (assoc get :uri "/v1/draftsets")]
    (tc/with-system [{handler :drafter.handler/app}
                     "test-system.edn"]
      (are [req identity status]
        (= status (:status (handler (if identity
                                      (tc/with-identity identity req)
                                      req))))
        whitelisted    nil            200
        whitelisted    test-norole    200
        whitelisted    test-access    200
        whitelisted    test-editor    200
        whitelisted    test-publisher 200
        get-public     nil            200
        get-public     test-norole    200
        get-public     test-access    200
        get-public     test-editor    200
        get-public     test-publisher 200
        options-public nil            200
        options-public test-norole    200
        options-public test-access    200
        options-public test-editor    200
        options-public test-publisher 200
        list-draftsets nil            401 ;; Unauthorized, requires editor
        list-draftsets test-norole    403 ;; Forbidden, requires editor
        list-draftsets test-access    403 ;; Forbidden, requires editor
        list-draftsets test-editor    200
        list-draftsets test-publisher 200))
    (tc/with-system [{handler :drafter.handler/app}
                     "global-auth-test-system.edn"]
      (are [req identity status]
        (= status (:status (handler (if identity
                                      (tc/with-identity identity req)
                                      req))))
        whitelisted    nil            200
        whitelisted    test-norole    200
        whitelisted    test-access    200
        whitelisted    test-editor    200
        whitelisted    test-publisher 200
        get-public     nil            401 ;; Unauthorized, requires access
        get-public     test-norole    403 ;; Forbidden, requires access
        get-public     test-access    200
        get-public     test-editor    200
        get-public     test-publisher 200
        options-public nil            200
        options-public test-norole    200
        options-public test-access    200
        options-public test-editor    200
        options-public test-publisher 200
        list-draftsets nil            401 ;; Unauthorized, requires editor
        list-draftsets test-norole    403 ;; Forbidden, requires editor
        list-draftsets test-access    403 ;; Forbidden, requires editor
        list-draftsets test-editor    200
        list-draftsets test-publisher 200))))
