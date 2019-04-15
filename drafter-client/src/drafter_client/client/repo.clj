(ns drafter-client.client.repo
  (:require [grafter.rdf.repository :as repo]
            [drafter-client.client.auth :as auth]
            [drafter-client.client.draftset :as draftset])
  (:import (java.net URI)))

(defn- ->query-url [drafter-uri context]
  {:pre [(or (draftset/live? context)
             (draftset/draft? context))]}
  (if (draftset/live? context)
    (format "%s/sparql/live" drafter-uri)
    (format "%s/draftset/%s/query?union-with-live=true"
            drafter-uri (draftset/id context))))

(defn make-repo [drafter-uri context jws-key user]
  {:pre [(instance? URI drafter-uri)]}
  (let [query-url (->query-url drafter-uri context)
        auth-header-val (auth/jws-auth-header-for jws-key user)]
    (doto (repo/sparql-repo query-url)
      (.setAdditionalHttpHeaders {"Authorization" auth-header-val}))))
