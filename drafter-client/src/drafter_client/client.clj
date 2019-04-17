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
            [ring.util.io :refer [piped-input-stream]]))

(alias 'c 'clojure.core)

(def default-format
  {:date-format     "yyyy-MM-dd"
   :datetime-format "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"})

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

(defn- json [data]
  (let [opts {:date-format (:datetime-format default-format)}]
    (json/generate-string data opts)))

(defn- n*->stream [format n*]
  (piped-input-stream
   (fn [output-stream]
     (pr/add (rio/rdf-writer output-stream :format format) n*))))

(defn- grafter->format-stream [content-type data]
  (let [format (mimetype->rdf-format content-type)]
    (n*->stream format data)))

(defn- read-body [content-type body]
  {:pre [(instance? java.io.InputStream body)]}
  (let [rdf-format (mimetype->rdf-format content-type)]
    (rio/statements body :format rdf-format)))

(defn ->repo [client user context]
  (repo/make-repo client context user {}))

(alter-var-root
 #'clojure.walk/keywordize-keys
 (constantly
  (fn [m]
    (let [f (fn [[k v]] (if (string? k) [(keyword k) v] [k v]))]
      (clojure.walk/postwalk (fn [x] (if (map? x) (into x (map f x)) x)) m)))))

(defn body-for
  ([client route user]
   (body-for client route user {}))
  ([client route user params]
   (-> (intercept client (auth/jws-auth client user))
       (martian/response-for route params)
       (:body))))

(defn draftsets
  "List available Draftsets"
  [client user]
  (->> (i/get client i/get-draftsets user)
       (map json-draftset->draftset)))

(defn new-draftset
  "Create a new Draftset"
  [client user name description]
  (-> client
      (i/get i/create-draftset user :display-name name :description description)
      (json-draftset->draftset)))

(defn remove-draftset
  "Delete the Draftset and its data"
  [client user draftset]
  (-> client
      (i/get i/delete-draftset user (draftset/id draftset))
      (->async-job)))

(defn add
  "Append the supplied RDF data to this Draftset"
  ([client user draftset quads]
   (-> client
       (i/content-type "application/n-quads")
       (i/get i/put-draftset-data user (draftset/id draftset) quads)
       (->async-job)))
  ([client user draftset graph triples]
   (let [id (draftset/id draftset)]
     (-> client
         (i/content-type "application/n-triples")
         (i/get i/put-draftset-data user id triples :graph graph)
         (->async-job)))))

(defn get
  "Access the quads inside this Draftset"
  ([client user draftset]
   (-> client
       (i/accept "application/n-quads")
       (i/get i/get-draftset-data user (draftset/id draftset))))
  ([client user draftset graph]
   (-> client
       (i/accept "application/n-triples")
       (i/get i/get-draftset-data user (draftset/id draftset) :graph graph))))

(defn refresh-job
  "Poll to see if asynchronous job has finished"
  [client user job]
  (-> client
      (i/get i/status-job-finished user (:job-id job))
      (try (catch clojure.lang.ExceptionInfo e
             (let [{:keys [body status]} (ex-data e)]
               (if (= status 404)
                 (json/parse-string body keyword)
                 (throw e)))))
      (->async-job)))

(defn resolve-job
  "Wait until asynchronous `job` has finished"
  [client user job]
  (let [job* (refresh-job client user job)]
    (cond
      (job-complete? job*) ::completed
      (job-in-progress? job*) (do (Thread/sleep 500)
                                  ;; Recur with the original
                                  (recur client user job)))))

(defn create
  "Create a Drafter client for `drafter-uri`"
  [drafter-uri & {:keys [batch-size jws-key version]}]
  (let [version (or version "v1")
        swagger-json "swagger/swagger.json"
        bencoder (fn [content-type]
                   {:encode (partial grafter->format-stream content-type)
                    :decode (partial read-body content-type)
                    :as :stream})
        jencoder {:encode json :decode #(encoders/json-decode % keyword)}
        encoders (assoc (encoders/default-encoders)
                        "application/json" jencoder
                        "application/n-quads" (bencoder "application/n-quads")
                        "application/n-triples" (bencoder "application/n-triples"))
        interceptors (conj martian/default-interceptors
                           (interceptors/encode-body encoders)
                           (interceptors/coerce-response encoders)
                           martian-http/perform-request)]
    (log/debugf "Making Drafter client with batch size %d for Drafter: %s"
                batch-size drafter-uri)
    (when (and drafter-uri jws-key)
      (-> (format "%s/%s" drafter-uri swagger-json)
          (martian-http/bootstrap-swagger {:interceptors interceptors})
          (->DrafterClient jws-key batch-size)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integrant ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmethod ig/init-key :drafter-client/client
  [_ {:keys [drafter-uri jws-key batch-size]}]
  (create drafter-uri :jws-key jws-key :batch-size batch-size))

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
