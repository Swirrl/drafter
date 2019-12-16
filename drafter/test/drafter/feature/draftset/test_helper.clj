(ns drafter.feature.draftset.test-helper
  (:require [clojure.java.io :as io]
            [clojure.test :refer [is]]
            [drafter.feature.draftset.create-test :as ct]
            [drafter.test-common :as tc]
            [drafter.user :as user]
            [drafter.user-test :refer [test-editor test-publisher]]
            [drafter.util :as util]
            [grafter-2.rdf.protocols :refer [add]]
            [grafter-2.rdf4j.formats :as formats]
            [grafter-2.rdf4j.io :refer [rdf-writer statements]]
            [schema.core :as s]
            [drafter.async.jobs :as async]
            [clojure.java.io :as io]
            [drafter.util :as util]))

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

(defn is-client-error-response?
  "Whether the given ring response map represents a client error."
  [{:keys [status] :as response}]
  (and (>= status 400)
       (< status 500)))

(defn create-submit-to-role-request [user draftset-location role]
  (tc/with-identity user {:uri (str draftset-location "/submit-to")
                          :request-method :post
                          :params {:role (name role)}}))

(defn create-draftset-through-api
  ([handler] (create-draftset-through-api handler test-editor))
  ([handler user] (create-draftset-through-api handler user nil))
  ([handler user display-name] (create-draftset-through-api handler user display-name nil))
  ([handler user display-name description]
   (let [request (ct/create-draftset-request user display-name description)
         {:keys [headers] :as response} (handler request)]
     (ct/assert-is-see-other-response response)
     (get headers "Location"))))

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

(defn submit-draftset-to-role-through-api [handler user draftset-location role]
  (let [response (handler (create-submit-to-role-request user draftset-location role))]
    (tc/assert-is-ok-response response)))

(defn delete-draftset-graph-request [user draftset-location graph-to-delete]
  (tc/with-identity user {:uri (str draftset-location "/graph") :request-method :delete :params {:graph (str graph-to-delete)}}))

(defn delete-draftset-graph-through-api [handler user draftset-location graph-to-delete]
  (let [delete-graph-request (delete-draftset-graph-request user draftset-location graph-to-delete)
        {:keys [body] :as delete-graph-response} (handler delete-graph-request)]
    (tc/assert-is-ok-response delete-graph-response)
    (tc/assert-schema Draftset body)
    body))

(defn statements->input-stream [statements format]
  (let [bos (java.io.ByteArrayOutputStream.)
        serialiser (rdf-writer bos :format format)]
    (add serialiser statements)
    (java.io.ByteArrayInputStream. (.toByteArray bos))))

(defn append-to-draftset-request [user draftset-location data-stream content-type]
  (tc/with-identity user
    {:uri (str draftset-location "/data")
     :request-method :put
     :body data-stream
     :headers {"content-type" content-type}}))

(defn statements->append-request [user draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)]
    (append-to-draftset-request user draftset-location input-stream (.getDefaultMIMEType (formats/->rdf-format format)))))

(defn append-quads-to-draftset-through-api [handler user draftset-location quads]
  (let [request (statements->append-request user draftset-location quads :nq)
        response (handler request)]
    (tc/await-success (get-in response [:body :finished-job]))))

(defn make-append-data-to-draftset-request [handler user draftset-location data-file-path]
  (with-open [fs (io/input-stream data-file-path)]
    (let [request (append-to-draftset-request user draftset-location fs "application/x-trig")]
      (handler request))))

(defn create-publish-request
  ([draftset-location user]
   (create-publish-request draftset-location user nil))
  ([draftset-location user metadata]
   (tc/with-identity user {:uri (str draftset-location "/publish")
                           :request-method :post
                           :params (if metadata {:metadata metadata} {})})))

(defn publish-draftset-through-api [handler draftset-location user]
  (let [publish-request (create-publish-request draftset-location user)
        publish-response (handler publish-request)]
    (tc/await-success (:finished-job (:body publish-response)))))

(defn publish-quads-through-api [handler quads]
  (let [draftset-location (create-draftset-through-api handler test-publisher)]
    (append-quads-to-draftset-through-api handler test-publisher draftset-location quads)
    (publish-draftset-through-api handler draftset-location test-publisher)))

(defn eval-statement [s]
  (util/map-values str s))

(defn eval-statements [ss]
  (map eval-statement ss))

(defn concrete-statements [source format]
  (eval-statements (statements source :format format)))

(defn get-draftset-quads-accept-request [draftset-location user accept union-with-live?-str]
  (tc/with-identity user
    {:uri (str draftset-location "/data")
     :request-method :get
     :headers {"accept" accept}
     :params {:union-with-live union-with-live?-str}}))

(defn get-draftset-quads-request [draftset-location user format union-with-live?-str]
  (get-draftset-quads-accept-request draftset-location user (.getDefaultMIMEType (formats/->rdf-format format)) union-with-live?-str))

(defn get-draftset-quads-through-api
  ([handler draftset-location user]
   (get-draftset-quads-through-api handler draftset-location user "false"))
  ([handler draftset-location user union-with-live?]
   (let [data-request (get-draftset-quads-request draftset-location user :nq union-with-live?)
         data-response (handler data-request)]
     (tc/assert-is-ok-response data-response)
     (concrete-statements (:body data-response) :nq))))

(defn get-draftset-graph-triples-through-api [handler draftset-location user graph union-with-live?-str]
  (let [data-request {:uri            (str draftset-location "/data")
                      :request-method :get
                      :headers        {"accept" "application/n-triples"}
                      :params         {:union-with-live union-with-live?-str :graph (str graph)}}
        data-request (tc/with-identity user data-request)
        {:keys [body] :as data-response} (handler data-request)]
    (tc/assert-is-ok-response data-response)
    (concrete-statements body :nt)))

;;TODO: Get quads through query of live endpoint? This depends on
;;'union with live' working correctly
(defn get-live-quads-through-api [handler]
  (let [tmp-ds (create-draftset-through-api handler test-editor)]
    (get-draftset-quads-through-api handler tmp-ds test-editor "true")))

(defn assert-live-quads [handler expected-quads]
  (let [live-quads (get-live-quads-through-api handler)]
    (is (= (set (eval-statements expected-quads)) (set live-quads)))))

(defn await-delete-statements-response [response]
  (let [job-result (tc/await-success (get-in response [:body :finished-job]))]
    (get-in job-result [:details :draftset])))

(defn create-delete-quads-request [user draftset-location input-stream format]
  (tc/with-identity user {:uri (str draftset-location "/data")
                          :request-method :delete
                          :body input-stream
                          :headers {"content-type" format}}))

(defn create-delete-statements-request [user draftset-location statements format]
  (let [input-stream (statements->input-stream statements format)]
    (create-delete-quads-request user draftset-location input-stream (.getDefaultMIMEType (formats/->rdf-format format)))))

(defn create-delete-triples-request [user draftset-location statements graph]
  (assoc-in (create-delete-statements-request user draftset-location statements :nt)
            [:params :graph] (str graph)))

(defn delete-triples-through-api [handler user draftset-location triples graph]
  (-> (create-delete-triples-request user draftset-location triples graph)
      (handler)
      (await-delete-statements-response)))

(defn delete-quads-through-api [handler user draftset-location quads]
  (let [delete-request (create-delete-statements-request user draftset-location quads :nq)
        delete-response (handler delete-request)]
    (await-delete-statements-response delete-response)))

(defn delete-draftset-triples-through-api [handler user draftset-location triples graph]
  (let [delete-request (create-delete-statements-request user draftset-location triples :nt)
        delete-request (assoc-in delete-request [:params :graph] (str graph))
        delete-response (handler delete-request)]
    (await-delete-statements-response delete-response)))

(defn get-draftset-info-request [draftset-location user]
  (tc/with-identity user {:uri draftset-location :request-method :get}))

(defn get-draftset-info-through-api [handler draftset-location user]
  (let [{:keys [body] :as response} (handler (get-draftset-info-request draftset-location user))]
    (tc/assert-is-ok-response response)
    (tc/assert-schema Draftset body)
    body))

(defn append-data-to-draftset-through-api [handler user draftset-location draftset-data-file]
  (let [append-response (make-append-data-to-draftset-request handler user draftset-location draftset-data-file)]
    (tc/await-success (:finished-job (:body append-response)))))

(defn statements->append-triples-request [user draftset-location triples graph]
  (-> (statements->append-request user draftset-location triples :nt)
      (assoc-in [:params :graph] (str graph))))

(defn append-triples-to-draftset-through-api [handler user draftset-location triples graph]
  (let [request (statements->append-triples-request user draftset-location triples graph)
        response (handler request)]
    (tc/await-success (get-in response [:body :finished-job]))))
