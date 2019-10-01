(ns drafter-client.client.repo
  (:require [cemerick.url :as url]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.client.impl :as i]
            [grafter-2.rdf4j.repository :as repo]
            [martian.core :as martian]))

(defn make-repo [client context token params]
  (let [params (update params :query (fnil identity ""))
        [endpoint params] (if (draftset/live? context)
                            [:get-query-live params]
                            [:get-query-draftset
                             (assoc params
                                    :id (str (draftset/id context))
                                    :union-with-live true)])
        {:keys [url query-params headers] :as req}
        (-> (i/set-bearer-token client token)
            (martian/request-for endpoint params))
        query-params (dissoc query-params :query)]
    (doto (repo/sparql-repo (str url \? (url/map->query query-params)))
      (.setAdditionalHttpHeaders (select-keys headers ["Authorization"])))))
