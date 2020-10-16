(ns drafter-client.client.protocols)

(deftype DrafterClient [martian opts auth-provider auth0]
  ;; Wrap martian in a type so that:
  ;; a) we don't leak the auth0 client
  ;; b) we don't expose the martian impl to the "system"
  ;; We can still get at the pieces if necessary due to the ILookup impl.
  clojure.lang.ILookup
  (valAt [this k] (.valAt this k nil))
  (valAt [this k default]
    (case k
      :martian martian
      :auth0 auth0
      :auth-provider auth-provider
      (or (get opts k)
          (.valAt martian k default)))))

(def drafter-client?
  "Test if value is an instance of DrafterClient"
  (partial instance? DrafterClient))

(defprotocol AuthorizationProvider
  "Necessary internal protocol to implement in order to swap
  authorization inside drafter-client."
  (authorization-header [t] "Return the authorization header for this auth method")
  (interceptor [t] "Return the interceptor for this auth provider"))
