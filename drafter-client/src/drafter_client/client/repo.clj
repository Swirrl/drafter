(ns drafter-client.client.repo
  (:require [drafter-client.client.auth :as auth]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.client.impl :refer [intercept]]
            [grafter-2.rdf4j.repository :as repo]
            [martian.core :as martian]))

(defn make-repo [client context {:keys [user] :as params}]
  (let [params (merge {:query ""} params)
        [endpoint params] (if (draftset/live? context)
                            [:get-query-live params]
                            [:get-query-draftset
                             (assoc params :id (str (draftset/id context)))])
        {:keys [url headers] :as req}
        (-> (intercept client (auth/jws-auth client user))
            (martian/request-for endpoint params))]
    (doto (repo/sparql-repo url)
      (.setAdditionalHttpHeaders (select-keys headers ["Authorization"])))))
