(ns drafter-client.client.swagger
  "A wrapper around the generated Swagger client, to make things a little bit
   nicer"
  (:require [drafter-client.client.auth :as auth]
            [drafter-swagger-client.core :as swagger]
            [drafter-swagger-client.api.draftsets :as draftsets]
            [drafter-swagger-client.api.updating-data :as data]
            [drafter-swagger-client.api.jobs :as jobs]
            [drafter-swagger-client.api.querying :as query]
            [grafter.rdf.io :refer [rdf-serializer]]
            [grafter.rdf :as rdf]
            [grafter.rdf.protocols :as pr]
            [grafter.rdf.formats :as gr-format]
            [ring.util.io :as ring-io]))

(defmacro call [[uri jws-key user] & body]
  `(swagger/with-api-context
     {:base-url ~uri
      :debug false
      :auths {"jws-auth" (auth/jws-auth-header-for ~jws-key ~user)}}
     (do
       ~@body)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Draftsets ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-draftsets [uri jws-key user]
  (call [uri jws-key user]
        (draftsets/draftsets-get)))

(defn make-draftset [uri jws-key user name description]
  (call [uri jws-key user]
        (draftsets/draftsets-post {:display-name name
                                   :description description})))

(defn delete-draftset [uri jws-key user id]
  {:pre [(uuid? id)]}
  (call [uri jws-key user]
        (draftsets/draftset-id-delete id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Data ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- json [data]
  (let [opts {:date-format (:datetime-format swagger/*api-context*)}]
    (cheshire.core/generate-string data opts)))

(defn- n*->stream [format n*]
  (ring-io/piped-input-stream
   (fn [output-stream]
     (pr/add (rdf-serializer output-stream
                             :format format)
             n*))))

(defn- grafter->format-stream [content-type data]
  (let [format (gr-format/mimetype->rdf-format content-type)]
    (n*->stream format data)))

(defn useful-serialize
  "Serialize data based on the content type."
  [data content-type]
  (cond
    (swagger/json-mime? content-type) (json data)
    :else (grafter->format-stream content-type data)))

(defn- read-body [content-type body]
  {:pre [(instance? java.io.InputStream body)]}
  (let [rdf-format (gr-format/mimetype->rdf-format content-type)]
    (rdf/statements body :format rdf-format)))

(defn useful-deserialize
  "Deserialize data based on the content type."
  [{:keys [body] {:keys [content-type]} :headers}]
  (cond
    (swagger/json-mime? content-type) (swagger/deserialize body)
    :else (read-body content-type body)))

(defn add-data
  ([uri jws-key user draftset-id quads]
   (with-redefs [swagger/serialize useful-serialize]
     (call [uri jws-key user]
           (data/draftset-id-data-put draftset-id quads))))
  ([uri jws-key user draftset-id graph triples]
   (with-redefs [swagger/serialize useful-serialize]
     (call [uri jws-key user]
           (data/draftset-id-data-put draftset-id triples {:graph graph})))))

(defn get-data
  ;; We need to return the body as a stream here
  ;; (client/get "http://example.com/bigrequest.html" {:as :stream})
  ([uri jws-key user draftset-id]
   (with-redefs [swagger/deserialize useful-deserialize]
     (call [uri jws-key user]
           (query/draftset-id-data-get draftset-id))))
  ([uri jws-key user draftset-id graph]
   (with-redefs [swagger/deserialize useful-deserialize]
     (call [uri jws-key user]
           (query/draftset-id-data-get draftset-id {:graph graph})))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Async Jobs ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-status [uri jws-key user job-id]
  (try (call [uri jws-key user]
             (jobs/status-finished-jobs-jobid-get job-id))
       (catch clojure.lang.ExceptionInfo e
         (let [data (ex-data e)]
           (swagger/deserialize data)))))
