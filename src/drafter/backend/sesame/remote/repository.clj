(ns drafter.backend.sesame.remote.repository
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [drafter.rdf DrafterSPARQLRepository]))

(defn get-required-environment-variable [var-key env-map]
  (if-let [ev (var-key env-map)]
    ev
    (let [var-name (str/upper-case (str/replace (name var-key) \- \_))
          message (str "Missing required key "
                       var-key
                       " in environment map. Ensure you export an environment variable "
                       var-name
                       " or define "
                       var-key
                       " in the relevant profile in profiles.clj")]
      (do
        (log/error message)
        (throw (RuntimeException. message))))))

(defn create-sparql-repository [query-endpoint update-endpoint]
  "Creates a new SPARQL repository with the given query and update
  endpoints."
  (let [repo (DrafterSPARQLRepository. query-endpoint update-endpoint)]
      (.initialize repo)
      (log/info "Initialised repo at QUERY=" query-endpoint ", UPDATE=" update-endpoint)
      repo))

(defn create-repository-for-environment [env-map]
  "Creates a new SPARQL repository with the query and update endpoints
  configured in the given environment map."
  (let [query-endpoint (get-required-environment-variable :sparql-query-endpoint env-map)
        update-endpoint (get-required-environment-variable :sparql-update-endpoint env-map)]
    (create-sparql-repository query-endpoint update-endpoint)))
