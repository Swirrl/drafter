(ns drafter-client.client.auth
  (:require [drafter-client.auth.basic-auth :as ba]
            [drafter-client.auth.auth0 :as dcauth0]))

(def ^{:deprecated "Moved to drafter-client.auth.auth0/get-client-id-token"}
  get-client-id-token dcauth0/get-client-id-token)

(defn ^{:deprecated "Use :drafter-client.auth/basic-auth auth provider instead"}
  basic-auth [user password]
  {:name ::auth-header
   :enter (fn [ctx]
            (assoc-in ctx [:request :headers "Authorization"]
                      (ba/basic-auth-header user password)))})
