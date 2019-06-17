(ns drafter-client.client
  (:refer-clojure :exclude [name type get])
  (:require [cheshire.core :as json]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [drafter-client.client.auth :as auth]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.client.impl :as i :refer [->DrafterClient intercept]]
            [drafter-client.client.repo :as repo]
            [grafter-2.rdf.protocols :as pr]
            [grafter-2.rdf4j.formats :refer [mimetype->rdf-format]]
            [grafter-2.rdf4j.io :as rio]
            [integrant.core :as ig]
            [martian.clj-http :as martian-http]
            [martian.core :as martian]
            [martian.encoders :as encoders]
            [martian.interceptors :as interceptors]
            [schema.core :as schema]
            [ring.util.codec :refer [form-encode form-decode]]
            [ring.util.io :refer [piped-input-stream]]))

(alias 'c 'clojure.core)

(def live draftset/live)

(defrecord AsyncJob [type job-id restart-id])

(defn- ->async-job [{:keys [type finished-job restart-id] :as rsp}]
  {:post [type restart-id]}
  (let [job-id (some-> finished-job
                       (str/split #"/")
                       last
                       java.util.UUID/fromString)]
    (->AsyncJob type job-id (java.util.UUID/fromString restart-id))))

(defn job-complete? [{:keys [type job-id] :as async-job}]
  {:pre [(instance? AsyncJob async-job)]}
  (and (nil? job-id)
       (= "ok" type)))

(defn job-in-progress? [{:keys [type job-id] :as async-job}]
  {:pre [(instance? AsyncJob async-job)]}
  (= "not-found" type))

(defn- json-draftset->draftset [ds]
  (let [{:keys [id display-name description]} ds
        id (java.util.UUID/fromString id)]
    (draftset/->draftset id display-name description)))

(defn ->repo [client access-token context]
  (repo/make-repo client context access-token {}))

(defn draftsets
  "List available Draftsets"
  [client access-token]
  (->> (i/get client i/get-draftsets access-token)
       (map json-draftset->draftset)))

(defn new-draftset
  "Create a new Draftset"
  [client access-token name description]
  (-> client
      (i/get i/create-draftset access-token :display-name name :description description)
      (json-draftset->draftset)))

(defn remove-draftset
  "Delete the Draftset and its data"
  [client access-token draftset]
  (-> client
      (i/get i/delete-draftset access-token (draftset/id draftset))
      (->async-job)))

(defn add
  "Append the supplied RDF data to this Draftset"
  ([client access-token draftset quads]
   (-> client
       (i/set-content-type "application/n-quads")
       (i/get i/put-draftset-data access-token (draftset/id draftset) quads)
       (->async-job)))
  ([client access-token draftset graph triples]
   (let [id (draftset/id draftset)]
     (-> client
         (i/set-content-type "application/n-triples")
         (i/get i/put-draftset-data access-token id triples :graph graph)
         (->async-job)))))

(defn get
  "Access the quads inside this Draftset"
  ([client access-token draftset]
   (-> client
       (i/accept "application/n-quads")
       (i/get i/get-draftset-data access-token (draftset/id draftset))))
  ([client access-token draftset graph]
   (-> client
       (i/accept "application/n-triples")
       (i/get i/get-draftset-data access-token (draftset/id draftset) :graph graph))))

(defn refresh-job
  "Poll to see if asynchronous job has finished"
  [client access-token job]
  (-> client
      (i/get i/status-job-finished access-token (:job-id job))
      (try (catch clojure.lang.ExceptionInfo e
             (let [{:keys [body status]} (ex-data e)]
               (if (= status 404)
                 (json/parse-string body keyword)
                 (throw e)))))
      (->async-job)))

(defn resolve-job
  "Wait until asynchronous `job` has finished"
  [client access-token job]
  (let [job* (refresh-job client access-token job)]
    (cond
      (job-complete? job*) ::completed
      (job-in-progress? job*) (do (Thread/sleep 500)
                                  ;; Recur with the original
                                  (recur client access-token job)))))

(defn web-client
  "Create a Drafter client for `drafter-uri` where the (web-)client will pass an
  access-token to each request."
  [drafter-uri & {:keys [batch-size version]}]
  (let [version (or version "v1")
        swagger-json "swagger/swagger.json"]
    (log/debugf "Making Drafter web-client with batch size %d for Drafter: %s"
                batch-size drafter-uri)
    (when (seq drafter-uri)
      (-> (format "%s/%s" drafter-uri swagger-json)
          (martian-http/bootstrap-swagger {:interceptors i/default-interceptors})
          (->DrafterClient batch-size nil)))))

(defn cli-client
  "Create a Drafter client for `drafter-uri` that will request tokens from Auth0
  for the (cli-)client."
  [drafter-uri
   & {:keys [batch-size version auth0-endpoint client-id client-secret]}]
  (let [version (or version "v1")
        swagger-json "swagger/swagger.json"
        auth0 (auth/client auth0-endpoint client-id client-secret)]
    (log/debugf "Making Drafter client with batch size %d for Drafter: %s"
                batch-size drafter-uri)
    (when (and drafter-uri auth0)
      (-> (format "%s/%s" drafter-uri swagger-json)
          (martian-http/bootstrap-swagger {:interceptors i/default-interceptors})
          (->DrafterClient batch-size auth0)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integrant ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod ig/init-key :drafter-client/client
  [_ {:keys [drafter-uri batch-size auth0-endpoint client-id client-secret]}]
  (web-client drafter-uri
              :batch-size batch-size
              :auth0-endpoint auth0-endpoint
              :client-id client-id
              :client-secret client-secret))

(defmethod ig/halt-key! :drafter-client/client [_ client]
  ;; Shutdown client.
  ;; TODO Anything to do here?
  ;; TOOD Will there be anything running in the background that we should wait
  ;; for?
  )

(s/def ::batch-size pos-int?)
;; TODO Find out if we can read this as a URI with integrant
(s/def ::drafter-uri (s/or :string string? :nil nil?))
(s/def ::auth0-endpoint (s/or :string string? :nil nil?))
(s/def ::client-id (s/or :string string? :nil nil?))
(s/def ::client-secret (s/or :string string? :nil nil?))

(defmethod ig/pre-init-spec :drafter-client/client [_]
  (s/keys :req-un [::batch-size
                   ::drafter-uri
                   ::auth0-endpoint
                   ::client-id
                   ::client-secret]))
