(ns drafter-client.client
  (:refer-clojure :exclude [name type get])
  (:require [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [integrant.core :as ig]
            [drafter-client.client.repo :as repo]
            [drafter-client.client.swagger :as swagger]
            [drafter-client.client.draftset :as draftset]
            [drafter-client.client.auth :as auth]
            [clojure.string :as str])
  (:import (java.net URI)
           (clojure.lang IDeref)))

(def live draftset/live)

(defprotocol IDrafterClient
  (->repo [this user context]
    "Get a SPARQL repository for the Drafter instance.

     Context can be either `live` or a Draft")
  (draftsets [this user])
  (new-draftset [this user name description])
  (remove-draftset [this user draftset])
  (add
    [this user draftset quads]
    [this user draftset graph triples])
  (get
    [this user draftset]
    [this user draftset graph])
  (refresh-job [this user job]))

(defrecord AsyncJob [type job-id restart-id])

(defn- ->async-job [{:keys [type finished-job restart-id] :as rsp}]
  {:post [type restart-id]}
  (let [job-id (some-> finished-job
                       (str/split #"/")
                       last
                       java.util.UUID/fromString)]
    (->AsyncJob type
                job-id
                (java.util.UUID/fromString restart-id))))



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

(defrecord DrafterClient [drafter-uri jws-key batch-size]
  IDrafterClient
  (->repo [_ user context]
    (repo/make-repo drafter-uri context jws-key user))
  (draftsets [_ user]
    (let [draftsets (swagger/get-draftsets drafter-uri jws-key user)]
      (map json-draftset->draftset draftsets)))
  (new-draftset [_ user name description]
    (-> (swagger/make-draftset drafter-uri jws-key user name description)
        json-draftset->draftset))
  (remove-draftset [_ user draftset]
    (->async-job
     (swagger/delete-draftset drafter-uri jws-key user (draftset/id draftset))))
  (add [this user draftset quads]
    (->async-job
     (swagger/add-data drafter-uri jws-key user (draftset/id draftset) quads)))
  (add [this user draftset graph triples]
    (->async-job
     (swagger/add-data drafter-uri jws-key user (draftset/id draftset) graph
                       triples)))
  (get [this user draftset]
    (swagger/get-data drafter-uri jws-key user (draftset/id draftset)))
  (get [this user draftset graph]
    (swagger/get-data drafter-uri jws-key user (draftset/id draftset) graph))
  (refresh-job [this user job]
    (let [job-id (:job-id job)]
      (->async-job (swagger/get-status drafter-uri jws-key user job-id))))
  Object
  (toString [this]
    (str (dissoc this :jws-key))))

(defn ->drafter-client [drafter-uri jws-key batch-size]
  {:pre [(instance? URI drafter-uri)
         (string? jws-key)
         (pos-int? batch-size)]}
  (log/debugf "Making Drafter client with batch size %d for Drafter: %s"
              batch-size drafter-uri)
  (->DrafterClient drafter-uri jws-key batch-size))

(defn resolve-job [client user job]
  (let [job* (refresh-job client user job)]
    (cond
      (job-complete? job*) ::completed
      (job-in-progress? job*) (do (Thread/sleep 500)
                                  ;; Recur with the original
                                  (recur client user job)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integrant ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod ig/init-key :drafter-client/client [_ opts]
  (let [{:keys [batch-size drafter-uri jws-key]} opts
        version "v1"]
    (when (and drafter-uri jws-key)
      (->drafter-client (URI. (format "%s/%s" drafter-uri version))
                        jws-key
                        batch-size))))

(defmethod ig/halt-key! :drafter-client/client [_ client]
  ;; Shutdown client.
  ;; TODO Anything to do here?
  ;; TOOD Will there be anything running in the background that we should wait
  ;; for?
  )

(s/def ::batch-size pos-int?)
;; TODO Find out if we can read this as a URI with integrant
(s/def ::drafter-uri (s/or :string string? :nil nil?))
(s/def ::jws-key (s/or :string string? :nil nil?))

(defmethod ig/pre-init-spec :drafter-client/client [_]
  (s/keys :req-un [::batch-size ::drafter-uri ::jws-key]))
