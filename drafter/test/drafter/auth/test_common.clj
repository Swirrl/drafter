(ns drafter.auth.test-common
  (:require [clojure.test :as t]
            [drafter.auth :as auth]))

(defn expect-authentication
  "Authenticates a request with the given authentication method. The
   request is expected to match."
  [auth-method request]
  (let [state (auth/parse-request auth-method request)]
    (t/is (some? state) "Expected authentication state from request")
    (auth/authenticate auth-method request state)))
