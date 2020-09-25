(ns drafter-client.client.protocols)

(defprotocol AuthorizationProvider
  "Necessary internal protocol to implement in order to swap
  authorization inside drafter-client."
  (authorization-header [t] "Return the authorization header for this auth method")
  (interceptor [t] "Return the interceptor for this auth provider"))
