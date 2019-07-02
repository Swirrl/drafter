(ns drafter-client.test-util.auth
  (:require [drafter-client.test-util.jwt :as jwt]
            [environ.core :refer [env]]))

(def system-user {:email "system@swirrl.com" :role "system"})

(defn system-token []
  (jwt/token (env :auth0-domain)
             (env :auth0-aud)
             "system@swirrl.com"
             "drafter:system"))

(def test-publisher {:email "publisher@swirrl.com" :role "publisher"})

(defn publisher-token []
  (jwt/token (env :auth0-domain)
             (env :auth0-aud)
             "publiser@swirrl.com"
             "drafter:publisher"))
