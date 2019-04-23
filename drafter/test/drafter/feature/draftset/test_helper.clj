(ns drafter.feature.draftset.test-helper
  (:require [drafter.feature.draftset.create-test :as ct]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor]]
            [grafter-2.rdf.protocols :refer [add]]
            [grafter-2.rdf4j.formats :as formats]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]]
            [swirrl-server.async.jobs :refer [finished-jobs]]
            [schema.core :as s]))

(def DraftsetWithoutTitleOrDescription
  {:id s/Str
   :changes {java.net.URI {:status (s/enum :created :updated :deleted)}}
   :created-at java.time.OffsetDateTime
   :updated-at java.time.OffsetDateTime
   :created-by s/Str
   (s/optional-key :current-owner) s/Str
   (s/optional-key :claim-role) s/Keyword
   (s/optional-key :claim-user) s/Str
   (s/optional-key :submitted-by) s/Str})

(def DraftsetWithoutDescription
  (assoc DraftsetWithoutTitleOrDescription :display-name s/Str))

(def draftset-with-description-info-schema
  (assoc DraftsetWithoutDescription :description s/Str))

(def Draftset
  (merge DraftsetWithoutTitleOrDescription
         {(s/optional-key :description) s/Str
          (s/optional-key :display-name) s/Str}))

(defn create-submit-to-role-request [user draftset-location role]
  (tc/with-identity user {:uri (str draftset-location "/submit-to")
                          :request-method :post
                          :params {:role (name role)}}))

(defn create-draftset-through-api [handler user]
  (-> test-editor ct/create-draftset-request handler :headers (get "Location")))

(defn submit-draftset-to-username-request [draftset-location target-username user]
  (tc/with-identity user {:uri (str draftset-location "/submit-to")
                          :request-method :post
                          :params {:user target-username}}))

(defn submit-draftset-to-user-request [draftset-location target-user user]
  (submit-draftset-to-username-request draftset-location (user/username target-user) user))

(defn submit-draftset-to-user-through-api [handler draftset-location target-user user]
  (let [request (submit-draftset-to-user-request draftset-location target-user user)
        response (handler request)]
    (tc/assert-is-ok-response response)))

(defn delete-draftset-graph-request [user draftset-location graph-to-delete]
  (tc/with-identity user {:uri (str draftset-location "/graph") :request-method :delete :params {:graph (str graph-to-delete)}}))

(defn delete-draftset-graph-through-api [handler user draftset-location graph-to-delete]
  (let [delete-graph-request (delete-draftset-graph-request user draftset-location graph-to-delete)
        {:keys [body] :as delete-graph-response} (handler delete-graph-request)]
    (tc/assert-is-ok-response delete-graph-response)
    (tc/assert-schema Draftset body)
    body))

(defn- statements->input-stream [statements format]
  (let [bos (java.io.ByteArrayOutputStream.)
        serialiser (rdf-writer bos :format format)]
    (add serialiser statements)
    (java.io.ByteArrayInputStream. (.toByteArray bos))))

(defn- append-to-draftset-request [user draftset-location data-stream content-type]
  (tc/with-identity user
    {:uri (str draftset-location "/data")
     :request-method :put
     :body data-stream
     :headers {"content-type" content-type}}))

(defn- statements->append-request [user draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)]
    (append-to-draftset-request user draftset-location input-stream (.getDefaultMIMEType (formats/->rdf-format format)))))

(defn append-quads-to-draftset-through-api [handler user draftset-location quads]
  (let [request (statements->append-request user draftset-location quads :nq)
        response (handler request)]
    (tc/await-success finished-jobs (get-in response [:body :finished-job]))))
