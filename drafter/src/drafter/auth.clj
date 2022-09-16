(ns drafter.auth
  (:require [drafter.responses :as response])
  (:import [clojure.lang ExceptionInfo]))

(defprotocol AuthenticationMethod
  "Represents a method of specifying user authentication within a request"

  (parse-request [this request]
    "Attempts to parse authentication data corresponding to this authentication
     method from an incoming request. This data will be passed to the authenticate
     method used to actually resolve a drafter user record. This method should
     not attempt to validate the data against a real user and should not throw
     exceptions. Should return nil if no data was found on the request for this
     authentication method.")

  (authenticate [this request state]
    "Takes the incoming request and the data returned by the parse-request method
     to resolve the authentication data to a valid drafter user. Should return the
     user record if successful, or throw an ExceptionInfo if the authentication
     failed.")

  (get-swagger-description [this]
    "Returns a high-level description of this authentication method along with any
     information required to use it within the Swagger UI. Should return a map containing
     the following keys:
       :heading - Brief heading for this authentication method
       :description - Markdown description of this method along with details for use in the UI")

  (get-swagger-key [this]
    "Returns a key to identify this authentication method within the swagger spec")

  (get-swagger-security-definition [this]
    "Returns the swagger security definition for this authentication method")

  (get-operation-swagger-security-requirement [this operation]
    "Returns a swagger security requirements object for the given operation")

  (get-swagger-ui-config [this]
    "Returns a map containing any extra configuration this authorisation method
     requires to configure the swagger UI."))

(defn authentication-failed-error
  "Constructs an Exception representing authentication failure"
  ([] (ex-info "Not authenticated" {:type ::authentication-failed}))
  ([msg] (ex-info msg {:type ::authentication-failed}))
  ([msg response]
   (ex-info msg {:type ::authentication-failed :response response})))

(defn authentication-failed
  "Throws an exception indicating the request could not be authenticated.
   The response to return can optionally be specified."
  ([] (throw (authentication-failed-error)))
  ([response] (throw (authentication-failed-error "Not authenticated" response))))

(defn authentication-failed-response
  "The response to return as the result of a request which failed due to the
   given authentication failure exception."
  [ex]
  (or (:response (ex-data ex))
      (response/unauthorized-response "Not authenticated.")))

(defn is-authentication-failed-error?
  "Whether the given Exception represents an authentication failure"
  [ex]
  (and (instance? ExceptionInfo ex)
       (= ::authentication-failed (:type (ex-data ex)))))

