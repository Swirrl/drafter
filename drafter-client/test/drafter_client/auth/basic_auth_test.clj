(ns drafter-client.auth.basic-auth-test
  (:require [drafter-client.auth.basic-auth :as sut]
            [drafter-client.client.protocols :as dcpr]
            [clojure.test :as t :refer [deftest testing is]]
            [integrant.core :as ig]))


(deftest basic-auth-provider-test
  (testing "basic-auth-provider"
    (let [bap (sut/map->BasicAuthProvider {:user "rick" :password "password"})]
      (testing "Interceptor"
        (let [f (:enter (dcpr/interceptor bap))
              modified-request (:request (f {:some :context}))]
          (is (= "Basic cmljazpwYXNzd29yZA==" (get-in modified-request [:headers "Authorization"])))))
      (testing "authorization-header"
        (is (= "Basic cmljazpwYXNzd29yZA==" (dcpr/authorization-header bap)))))))
