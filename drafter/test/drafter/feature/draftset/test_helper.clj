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
            [drafter.draftset :as ds]
            [drafter.draftset.spec]
            [drafter.endpoint :as ep]
            [clojure.java.io :as io]
            [drafter.util :as util]
            [martian.encoders :as enc]
            [clojure.string :as string]
            [drafter.backend.draftset :as draftset-backend]
            [drafter.backend.draftset.operations :as dsops]
            [grafter-2.rdf4j.repository :as repo]
            [drafter.backend.live :as live]
            [drafter.backend.draftset.graphs :as graphs]
            [grafter-2.rdf.protocols :as pr])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]
           [java.util.zip GZIPOutputStream]))

(defn is-client-error-response?
  "Whether the given ring response map represents a client error."
  [{:keys [status] :as response}]
  (and (>= status 400)
       (< status 500)))

(defn create-submit-to-permission-request [user draftset-location permission]
  (tc/with-identity user {:uri (str draftset-location "/submit-to")
                          :request-method :post
                          :params {:permission (name permission)}}))

;; The role parameter is deprecated
(defn create-submit-to-role-request [user draftset-location role]
  (tc/with-identity user {:uri (str draftset-location "/submit-to")
                          :request-method :post
                          :params {:role (name role)}}))

(defn create-share-with-permission-request [user draftset-location permission]
  (tc/with-identity user {:uri (str draftset-location "/share")
                          :request-method :post
                          :params {:permission (name permission)}}))

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

(defn share-draftset-with-username-request [draftset-location target-username user]
  (tc/with-identity user {:uri (str draftset-location "/share")
                          :request-method :post
                          :params {:user target-username}}))

(defn share-draftset-with-user-request [draftset-location target-user user]
  (share-draftset-with-username-request
   draftset-location (user/username target-user) user))

(defn unshare-draftset-request [draftset-location user]
  (tc/with-identity user {:uri (str draftset-location "/share")
                          :request-method :delete}))

(defn submit-draftset-to-user-through-api [handler draftset-location target-user user]
  (let [request (submit-draftset-to-user-request draftset-location target-user user)
        response (handler request)]
    (tc/assert-is-ok-response response)))

(defn submit-draftset-to-permission-through-api
  [handler user draftset-location permission]
  (let [response (handler (create-submit-to-permission-request user
                                                               draftset-location
                                                               permission))]
    (tc/assert-is-ok-response response)))

;; The role parameter is deprecated
(defn submit-draftset-to-role-through-api
  [handler user draftset-location role]
  (let [response (handler (create-submit-to-role-request user
                                                         draftset-location
                                                         role))]
    (tc/assert-is-ok-response response)))

(defn delete-draftset-graph-request [user draftset-location graph-to-delete]
  (tc/with-identity user {:uri (str draftset-location "/graph") :request-method :delete :params {:graph (str graph-to-delete)}}))

(defn delete-draftset-graph-through-api [handler user draftset-location graph-to-delete]
  (let [delete-graph-request (delete-draftset-graph-request user draftset-location graph-to-delete)
        {:keys [body] :as delete-graph-response} (handler delete-graph-request)]
    (tc/assert-is-ok-response delete-graph-response)
    (tc/assert-spec ::ds/Draftset body)
    body))

(defn statements->input-stream [statements format]
  (let [bos (ByteArrayOutputStream.)
        serialiser (rdf-writer bos :format format)]
    (add serialiser statements)
    (ByteArrayInputStream. (.toByteArray bos))))

(defn statements->gzipped-input-stream [statements format]
  (let [baos (ByteArrayOutputStream.)]
    (with-open [gzos (GZIPOutputStream. baos)]
      (add (rdf-writer gzos :format format) statements))
    (ByteArrayInputStream. (.toByteArray baos))))

(defn append-to-draftset-request [user draftset-location data-stream {:keys [content-type metadata]}]
  (tc/with-identity user
                    (cond-> {:uri (str draftset-location "/data")
                             :request-method :put
                             :body data-stream
                             :headers {"content-type" content-type}}
                            metadata (merge {:params {:metadata (enc/json-encode metadata)}}))))

(defn statements->append-request [user draftset-location statements {:keys [format] :as opts}]
  (let [input-stream (statements->input-stream statements format)]
    (append-to-draftset-request user
                                draftset-location
                                input-stream
                                (assoc opts :content-type
                                            (.getDefaultMIMEType (formats/->rdf-format format))))))

(defn append-quads-to-draftset-through-api [handler user draftset-location quads]
  (let [request (statements->append-request user draftset-location quads {:format :nq})
        response (handler request)]
    (or (some-> response
                (get-in [:body :finished-job])
                (tc/await-success))
        (throw (ex-info "No job-path present in response" response)))))

(defn make-append-data-to-draftset-request [handler user draftset-location data-file-path]
  (with-open [fs (io/input-stream data-file-path)]
    (let [request (append-to-draftset-request user draftset-location fs {:content-type "application/x-trig"})]
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

(defn request-public-endpoint-through-api
  "Submits a request for the public endpoint to a handler and returns the response"
  [handler]
  (handler {:uri "/v1/endpoint/public" :request-method :get}))

(defn get-public-endpoint-through-api
  "Submits a request for the public endpoint and returns the endpoint"
  [handler]
  (let [{:keys [body] :as response} (request-public-endpoint-through-api handler)]
    (tc/assert-is-ok-response response)
    (tc/assert-spec ::ep/Endpoint body)
    (:body response)))

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

(defn is-default-user-graph? [graph-uri]
  (let [gm (graphs/create-manager nil)]
    (graphs/user-graph? gm graph-uri)))

(defn is-user-quad?
  ([quad] (is-user-quad? (graphs/create-manager nil) quad))
  ([graph-manager quad]
   (graphs/user-graph? graph-manager (pr/context quad))))

(defn get-user-draftset-quads-through-api
  ([handler draftset-location user]
   (get-user-draftset-quads-through-api handler draftset-location user "false"))
  ([handler draftset-location user union-with-live?]
   (let [data-request (get-draftset-quads-request draftset-location user :nq union-with-live?)
         data-response (handler data-request)]
     (tc/assert-is-ok-response data-response)
     (let [source (:body data-response)
           quads (statements source :format :nq)
           gm (graphs/create-manager nil)
           user-quads (filter #(is-user-quad? gm %) quads)]
       (eval-statements user-quads)))))

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
(defn get-live-user-quads-through-api [handler]
  (let [tmp-ds (create-draftset-through-api handler test-editor)]
    (get-user-draftset-quads-through-api handler tmp-ds test-editor "true")))

(defn await-delete-statements-response [response]
  (let [job-result (tc/await-success (get-in response [:body :finished-job]))]
    (get-in job-result [:details :draftset])))

(defn create-delete-quads-request [user draftset-location input-stream {:keys [content-type metadata]}]
  (tc/with-identity user (cond-> {:uri (str draftset-location "/data")
                                  :request-method :delete
                                  :body input-stream
                                  :headers {"content-type" content-type}}
                                 metadata (merge {:params {:metadata (enc/json-encode metadata)}}))))

(defn create-delete-statements-request [user draftset-location statements {:keys [format] :as opts}]
  (let [input-stream (statements->input-stream statements format)]
    (create-delete-quads-request user
                                 draftset-location
                                 input-stream
                                 (assoc opts :content-type
                                             (.getDefaultMIMEType (formats/->rdf-format format))))))

(defn create-delete-triples-request [user draftset-location statements {:keys [graph] :as opts}]
  (assoc-in (create-delete-statements-request user draftset-location statements (merge {:format :nt} opts))
            [:params :graph] (str graph)))

(defn delete-triples-through-api [handler user draftset-location triples graph]
  (-> (create-delete-triples-request user draftset-location triples {:graph graph})
      (handler)
      (await-delete-statements-response)))

(defn delete-quads-through-api [handler user draftset-location quads]
  (let [delete-request (create-delete-statements-request user draftset-location quads {:format :nq})
        delete-response (handler delete-request)]
    (await-delete-statements-response delete-response)))

(defn delete-draftset-triples-through-api [handler user draftset-location triples graph]
  (let [delete-request (create-delete-statements-request user draftset-location triples {:format :nt
                                                                                         :graph graph})
        delete-request (assoc-in delete-request [:params :graph] (str graph))
        delete-response (handler delete-request)]
    (await-delete-statements-response delete-response)))

(defn get-draftset-info-request [draftset-location user]
  (tc/with-identity user {:uri draftset-location :request-method :get}))

(defn user-draftset-info-view
  "Returns a view of a draftset info return which contains only information about user graphs"
  [ds-info]
  (letfn [(user-graph? [graph-uri] (graphs/user-graph? (graphs/create-manager nil) graph-uri))
          (user-graph-changes [changes]
            (into {} (filter (fn [[graph-uri _graph-state]]
                               (user-graph? graph-uri))
                             changes)))]
    (update ds-info :changes user-graph-changes)))

(defn get-user-draftset-info-view-through-api [handler draftset-location user]
  (let [{:keys [body] :as response} (handler (get-draftset-info-request draftset-location user))]
    (tc/assert-is-ok-response response)
    (tc/assert-spec ::ds/Draftset body)
    (user-draftset-info-view body)))

(defn append-data-to-draftset-through-api [handler user draftset-location draftset-data-file]
  (let [append-response (make-append-data-to-draftset-request handler user draftset-location draftset-data-file)]
    (tc/await-success (:finished-job (:body append-response)))))

(defn statements->append-triples-request [user draftset-location triples {:keys [graph] :as opts}]
  (-> (statements->append-request user draftset-location triples (merge opts {:format :nt}))
      (assoc-in [:params :graph] (str graph))))

(defn append-triples-to-draftset-through-api [handler user draftset-location triples graph]
  (let [request (statements->append-triples-request user draftset-location triples {:graph graph})
        response (handler request)]
    (tc/await-success (get-in response [:body :finished-job]))))

(defn location->draftset-ref
  "Returns a DraftsetRef from the path returned from the API"
  [draftset-location]
  (let [draftset-id (last (string/split draftset-location #"/"))]
    (ds/->DraftsetId draftset-id)))

(defn- get-draftset-repo [backend draftset-location]
  (let [draftset-ref (location->draftset-ref draftset-location)]
    (draftset-backend/build-draftset-endpoint backend draftset-ref false)))

(defn get-draftset-user-quads
  "Returns all the quads within user graphs within a draftset"
  [backend draftset-location]
  (let [draft-repo (get-draftset-repo backend draftset-location)
        query (dsops/all-quads-query draft-repo)]
    (set (filter is-user-quad? (repo/evaluate query)))))

(defn get-draftset-graph-triples
  "Returns all the triples within a graph in a draftset"
  [backend draftset-location graph-uri]
  (let [draft-repo (get-draftset-repo backend draftset-location)
        query (dsops/all-graph-triples-query draft-repo graph-uri)]
    (set (repo/evaluate query))))

(defn get-live-graph-triples
  "Returns all the triples within a live graph"
  [repo graph-uri]
  (let [live-repo (live/live-endpoint-with-stasher repo)
        q (dsops/all-graph-triples-query live-repo graph-uri)]
    (set (repo/evaluate q))))
