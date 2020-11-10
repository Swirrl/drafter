(ns drafter-client.auth.auth0-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [drafter-client.auth.auth0.m2m :as sut]))

(deftest fetch-token-with-caching-test
  (let [state (atom {:call-count 0})
        token-state (atom nil)
        fake-token-fetcher (fn [arg]
                             (swap! state (fn [m] (-> m
                                                     (update :call-count inc)
                                                     (assoc :supplied-arg arg))))
                             {:access_token "fake-token"})

        fake-auth-provider {:state-atom token-state
                            :auth0-client {:fake :auth0-client}}]

    (testing "First call"
      (let [ret (sut/fetch-token-with-caching fake-auth-provider
                                              fake-token-fetcher)]

        (is (= ret {:access_token "fake-token"})
            "Returns token")

        (let [{:keys [call-count supplied-arg]} @state]

          (is (= call-count 1)
              "Side effecting function was called to fetch token")

          (is (= {:fake :auth0-client}
                 (:auth0 supplied-arg))
              "Function is supplied the :auth0-client on the :auth0 key"
              ;; NOTE this is done for legacy reasons, ideally we'd
              ;; use the :auth0-client key throughout.
              ))))

    (testing "Subsequent call(s)"
      (let [ret (sut/fetch-token-with-caching fake-auth-provider
                                              fake-token-fetcher)]

        (let [{:keys [call-count supplied-arg]} @state]

          (is (= call-count 1)
              "Side effecting function was not called again"))

        (is (= ret {:access_token "fake-token"})
            "Returns (cached) token")))))
