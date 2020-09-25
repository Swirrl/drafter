(ns drafter-client.auth.legacy-default
  (:require [drafter-client.client.protocols :as dcpr]
            [drafter-client.client.interceptors :as interceptor]
            [drafter-client.auth.auth0 :as dc-auth0]))


(defn legacy-config-warning! [client]
  ;; We don't have to enable the warning just yet, this is just to
  ;; capture the intention of some of this refactoring in the interim.
  ;;
  ;; At some point we can enable this, to encourage users to upgrade
  ;; to the new config style and interface.
  #_(println "This client is using a legacy code path, please change the interface through which you configure and authenticate." client)
  )


(defn with-auth0-default
  "NOTE: this is a default and a code path we want to deprecate. This
  code path is for clients that supply the access-token with every
  request they make, rather than clients that set the
  authorization/authentication mechanism via an
  authorization-provider.

  NOTE: this path still assumes PMD4-style auth0, it is just the
  interface and configuration style we want to deprecate, not the
  client."
  [client access-token]
  (legacy-config-warning! client)
  (interceptor/intercept client (dc-auth0/auth0-interceptor (str "Bearer " access-token))))
