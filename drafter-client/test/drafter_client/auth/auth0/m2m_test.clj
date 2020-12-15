(ns drafter-client.auth.auth0.m2m-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [drafter-client.client.protocols :as dcpr]
            [drafter-client.auth.auth0.m2m :as sut]))

(deftest fetch-token-with-caching-test
  (let [state (atom {:call-count 0})
        token-state (atom nil)
        auth0-client {:fake :auth0-client}
        fake-token-fetcher (fn [arg]
                             (swap! state (fn [m] (-> m
                                                     (update :call-count inc)
                                                     (assoc :supplied-arg arg))))
                             {:access_token "fake-token"})
        auth-provider (sut/build-auth-provider {:fetch-fn fake-token-fetcher
                                                :state-atom token-state
                                                :auth0-client auth0-client})]

    (testing "First call"
      (let [ret (dcpr/authorization-header auth-provider)]

        (is (= "Bearer fake-token" ret)
            "Returns token")

        (let [{:keys [call-count supplied-arg]} @state]

          (is (= call-count 1)
              "Side effecting function was called to fetch token")

          (is (= auth0-client
                 (:auth0 supplied-arg))
              "Function is supplied the :auth0-client on the :auth0 key"
              ;; NOTE this is done for legacy reasons, ideally we'd
              ;; use the :auth0-client key throughout.
              ))))

    (testing "Subsequent call(s)"
      (let [ret (dcpr/authorization-header auth-provider)
            {:keys [call-count]} @state]

        (is (= call-count 1)
            "Side effecting function was not called again")

        (is (= "Bearer fake-token" ret)
            "Returns (cached) token")))))
