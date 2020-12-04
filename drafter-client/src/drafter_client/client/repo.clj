(ns drafter-client.client.repo
  (:require [cemerick.url :as url]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.client.impl :as i]
            [grafter-2.rdf4j.repository :as repo]
            [martian.core :as martian]))

(defn- get-query-request
  "When constructing query and update requests the :query and :update parameters are
   required on the operation but should not be set in the query parameters for the
   corresponding query endpoint. Build a request with a dummy value for the specified
   query parameter and remove it from the output parameters."
  [martian route-name params query-key]
  (let [params (assoc params query-key "dummy")
        request (martian/request-for martian route-name params)]
    (update request :query-params dissoc query-key)))

(defn make-repo
  "Takes a DrafterClient and returns a grafter sparql-repo on it with
  the appropriate authorization derived from either the
  DrafterClient's :auth-provider or by using token if it is non-nil.

  Args are:

  client   - The DrafterClient to derive this repo from.

  endpoint - Either a draftset (as returned by get-draftset(s) ) or
             the value :drafter-client.client.draftset/live for the public
             endpoint.

  token    - An auth0 token or nil (if DrafterClient is configured with an :auth-provider)

  params   - A map of extra parameters to pass to the endpoint. "
  [client endpoint token params]
  (let [[query-endpoint-key update-endpoint-key params] (if (draftset/live? endpoint)
                                                          [:get-query-live nil params]
                                                          [:get-query-draftset
                                                           :post-update-draftset
                                                           (assoc params
                                                             :id (str (draftset/id endpoint))
                                                             :union-with-live true)])
        {query-url :url
         query-params :query-params
         headers :headers} (-> (i/with-authorization client token)
                               (get-query-request query-endpoint-key params :query))
        sparql-repo (if update-endpoint-key
                      (let [{update-url :url update-params :query-params} (get-query-request client update-endpoint-key params :update)]
                        (repo/sparql-repo (str query-url \? (url/map->query query-params))
                                          (str update-url \? (url/map->query update-params))))
                      (repo/sparql-repo (str query-url \? (url/map->query query-params))))]
    (.setAdditionalHttpHeaders sparql-repo (select-keys headers ["Authorization"]))
    sparql-repo))