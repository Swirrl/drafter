(ns drafter-swagger-client.api.draftsets
  (:require [drafter-swagger-client.core :refer [call-api check-required-params with-collection-format]])
  (:import (java.io File)))

(defn draftset-id-claim-put-with-http-info
  "Claim this draftset as your own
  Sets the Draftset's `current-owner` to be the same as the user
performing this operation.  This is necessary to prevent
other's from making changes to the data contained within the
Draftset.

Each role in the system has a pool of 0 or more claimable
draftsets associated with it.  Claimable draftsets are
draftsets in a pool where the rank of the pools role is less
than or equal to the user's role's rank."
  [id ]
  (check-required-params id)
  (call-api "/draftset/{id}/claim" :put
            {:path-params   {"id" id }
             :header-params {}
             :query-params  {}
             :form-params   {}
             :content-types ["application/json"]
             :accepts       ["application/json"]
             :auth-names    ["jws-auth"]}))

(defn draftset-id-claim-put
  "Claim this draftset as your own
  Sets the Draftset's `current-owner` to be the same as the user
performing this operation.  This is necessary to prevent
other's from making changes to the data contained within the
Draftset.

Each role in the system has a pool of 0 or more claimable
draftsets associated with it.  Claimable draftsets are
draftsets in a pool where the rank of the pools role is less
than or equal to the user's role's rank."
  [id ]
  (:data (draftset-id-claim-put-with-http-info id)))

(defn draftset-id-delete-with-http-info
  "Delete the Draftset and its data
  Deletes the draftset and its contents."
  [id ]
  (check-required-params id)
  (call-api "/draftset/{id}" :delete
            {:path-params   {"id" id }
             :header-params {}
             :query-params  {}
             :form-params   {}
             :content-types ["application/json"]
             :accepts       ["application/json"]
             :auth-names    ["jws-auth"]}))

(defn draftset-id-delete
  "Delete the Draftset and its data
  Deletes the draftset and its contents."
  [id ]
  (:data (draftset-id-delete-with-http-info id)))

(defn draftset-id-publish-post-with-http-info
  "Publish the specified Draftset
  Requests that this Draftset is published asynchronously to the live site.  If a job is successfully scheduled then an AsyncJob object will be returned."
  [id ]
  (check-required-params id)
  (call-api "/draftset/{id}/publish" :post
            {:path-params   {"id" id }
             :header-params {}
             :query-params  {}
             :form-params   {}
             :content-types ["application/json"]
             :accepts       ["application/json"]
             :auth-names    ["jws-auth"]}))

(defn draftset-id-publish-post
  "Publish the specified Draftset
  Requests that this Draftset is published asynchronously to the live site.  If a job is successfully scheduled then an AsyncJob object will be returned."
  [id ]
  (:data (draftset-id-publish-post-with-http-info id)))

(defn draftset-id-submit-to-post-with-http-info
  "Submit a Draftset to a user or role
  Submits this draftset for review and potential publication.
Draftsets are submitted directly to a user, or into a pool for
users of a given role.

Users with a role greater than or equal to the role the
draftset was submitted to can then lay claim to it."
  ([id ] (draftset-id-submit-to-post-with-http-info id nil))
  ([id {:keys [role user ]}]
   (check-required-params id)
   (call-api "/draftset/{id}/submit-to" :post
             {:path-params   {"id" id }
              :header-params {}
              :query-params  {"role" role "user" user }
              :form-params   {}
              :content-types ["application/json"]
              :accepts       ["application/json"]
              :auth-names    ["jws-auth"]})))

(defn draftset-id-submit-to-post
  "Submit a Draftset to a user or role
  Submits this draftset for review and potential publication.
Draftsets are submitted directly to a user, or into a pool for
users of a given role.

Users with a role greater than or equal to the role the
draftset was submitted to can then lay claim to it."
  ([id ] (draftset-id-submit-to-post id nil))
  ([id optional-params]
   (:data (draftset-id-submit-to-post-with-http-info id optional-params))))

(defn draftsets-get-with-http-info
  "List available Draftsets
  Lists draftsets visible to the user. The include parameter can
be used to filter the result list to just those owned by the
current user, or those not owned which can be claimed by the
current user. By default all owned and claimable draftsets are
returned."
  ([] (draftsets-get-with-http-info nil))
  ([{:keys [include ]}]
   (call-api "/draftsets" :get
             {:path-params   {}
              :header-params {}
              :query-params  {"include" include }
              :form-params   {}
              :content-types ["application/json"]
              :accepts       ["application/json"]
              :auth-names    ["jws-auth"]})))

(defn draftsets-get
  "List available Draftsets
  Lists draftsets visible to the user. The include parameter can
be used to filter the result list to just those owned by the
current user, or those not owned which can be claimed by the
current user. By default all owned and claimable draftsets are
returned."
  ([] (draftsets-get nil))
  ([optional-params]
   (:data (draftsets-get-with-http-info optional-params))))

(defn draftsets-post-with-http-info
  "Create a new Draftset
  Creates a new draftset in the database.

Optionally accepts query string parameters for a name and a description."
  ([] (draftsets-post-with-http-info nil))
  ([{:keys [display-name description ]}]
   (call-api "/draftsets" :post
             {:path-params   {}
              :header-params {}
              :query-params  {"display-name" display-name "description" description }
              :form-params   {}
              :content-types ["application/json"]
              :accepts       ["application/json"]
              :auth-names    ["jws-auth"]})))

(defn draftsets-post
  "Create a new Draftset
  Creates a new draftset in the database.

Optionally accepts query string parameters for a name and a description."
  ([] (draftsets-post nil))
  ([optional-params]
   (:data (draftsets-post-with-http-info optional-params))))

