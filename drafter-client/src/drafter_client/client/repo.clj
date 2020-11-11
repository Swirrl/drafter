(ns drafter-client.client.repo
  (:require [cemerick.url :as url]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.client.impl :as i]
            [drafter-client.client.interceptors :as interceptor]
            [grafter-2.rdf4j.repository :as repo]
            [martian.core :as martian]))

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
  (let [params (update params :query (fnil identity ""))
        [endpoint params] (if (draftset/live? endpoint)
                            [:get-query-live params]
                            [:get-query-draftset
                             (assoc params
                                    :id (str (draftset/id endpoint))
                                    :union-with-live true)])
        {:keys [url query-params headers]
         :as req} (-> (i/with-authorization client token)
                      (martian/request-for endpoint params))

        query-params (dissoc query-params :query)]
    (doto (repo/sparql-repo (str url \? (url/map->query query-params)))
      (.setAdditionalHttpHeaders (select-keys headers ["Authorization"])))))
