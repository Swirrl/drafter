(ns drafter-client.test-util.auth
  (:require [drafter.test-common :refer [token]]
            [drafter.user :refer [role->permissions]]
            [environ.core :refer [env]]))

(def system-user {:email "system@swirrl.com" :role "system"})

(defn system-token []
  (token (env :auth0-domain)
         (env :auth0-aud)
         "system@swirrl.com"
         "drafter:system"
         (role->permissions :system)))

(def test-publisher {:email "publisher@swirrl.com" :role "publisher"})

(defn publisher-token []
  (token (env :auth0-domain)
         (env :auth0-aud)
         "publisher@swirrl.com"
         "drafter:publisher"
         (role->permissions :publisher)))

(defn editor-token []
  (token (env :auth0-domain)
         (env :auth0-aud)
         "editor@swirrl.com"
         "drafter:editor"
         (role->permissions :editor)))
