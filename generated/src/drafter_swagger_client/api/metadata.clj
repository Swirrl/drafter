(ns drafter-swagger-client.api.metadata
  (:require [drafter-swagger-client.core :refer [call-api check-required-params with-collection-format]])
  (:import (java.io File)))

(defn draftset-id-get-with-http-info
  "Get information about a Draftset
  Returns metadata about the draftset.

NOTE"
  [id ]
  (check-required-params id)
  (call-api "/draftset/{id}" :get
            {:path-params   {"id" id }
             :header-params {}
             :query-params  {}
             :form-params   {}
             :content-types ["application/json"]
             :accepts       ["application/json"]
             :auth-names    ["jws-auth"]}))

(defn draftset-id-get
  "Get information about a Draftset
  Returns metadata about the draftset.

NOTE"
  [id ]
  (:data (draftset-id-get-with-http-info id)))

(defn draftset-id-put-with-http-info
  "Set metadata on Draftset
  Sets metadata properties on the draftset, allowing updates to
the draftsets title and description."
  ([id ] (draftset-id-put-with-http-info id nil))
  ([id {:keys [display-name description ]}]
   (check-required-params id)
   (call-api "/draftset/{id}" :put
             {:path-params   {"id" id }
              :header-params {}
              :query-params  {"display-name" display-name "description" description }
              :form-params   {}
              :content-types ["application/json"]
              :accepts       ["application/json"]
              :auth-names    ["jws-auth"]})))

(defn draftset-id-put
  "Set metadata on Draftset
  Sets metadata properties on the draftset, allowing updates to
the draftsets title and description."
  ([id ] (draftset-id-put id nil))
  ([id optional-params]
   (:data (draftset-id-put-with-http-info id optional-params))))

