(ns drafter-swagger-client.api.users
  (:require [drafter-swagger-client.core :refer [call-api check-required-params with-collection-format]])
  (:import (java.io File)))

(defn users-get-with-http-info
  "Gets all users
  Returns a JSON document containing an array of summary
documents, one for each known user."
  []
  (call-api "/users" :get
            {:path-params   {}
             :header-params {}
             :query-params  {}
             :form-params   {}
             :content-types ["application/json"]
             :accepts       ["application/json"]
             :auth-names    ["jws-auth"]}))

(defn users-get
  "Gets all users
  Returns a JSON document containing an array of summary
documents, one for each known user."
  []
  (:data (users-get-with-http-info)))

