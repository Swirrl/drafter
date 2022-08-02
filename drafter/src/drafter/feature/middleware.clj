(ns drafter.feature.middleware
  "Shared middlewares"
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.draftset :as ds]
            [drafter.middleware :as middleware]
            [drafter.responses :as response :refer [unprocessable-entity-response]]
            [grafter-2.rdf4j.repository :as repo]
            [integrant.core :as ig]
            [ring.util.response :as ring])
  (:import java.net.URI))

(defn- try-parse-uri [s]
  (try
    (URI. s)
    (catch Exception ex
      ex)))

(defn parse-graph-param-handler
  "Handle HTTP responses associated with graph query param on draftset
  and draftset-data routes."
  [required? inner-handler]
  (fn [request]
    (let [graph (get-in request [:params :graph])]
      (cond
        (some? graph)
        (let [uri-or-ex (try-parse-uri graph)]
          (if (instance? URI uri-or-ex)
            (inner-handler (assoc-in request [:params :graph] uri-or-ex))
            (unprocessable-entity-response "Valid URI required for graph parameter")))

        required?
        (unprocessable-entity-response "Graph parameter required")

        :else
        (inner-handler request)))))

(defn existing-draftset-handler [backend inner-handler]
  (fn [{{:keys [id]} :params :as request}]
    (let [draftset-id (ds/->DraftsetId id)]
      (if (dsops/draftset-exists? (repo/->connection backend) draftset-id)
        (let [updated-request (assoc-in request [:params :draftset-id] draftset-id)]
          (inner-handler updated-request))
        (ring/not-found "")))))

(defn restrict-to-draftset-owner
  "Middleware to enforce authentication and check the draftset is
  owned by the user making the request"
  [backend inner-handler]
  (fn [{user :identity {:keys [draftset-id]} :params :as request}]
    (if (dsops/is-draftset-owner? backend draftset-id user)
      (inner-handler request)
      (response/forbidden-response "Operation only permitted by draftset owner"))))

(defn wrap-as-draftset-owner
  [{:keys [:drafter/backend wrap-authenticate]}]
  (fn [permission handler]
    (middleware/wrap-authorize wrap-authenticate permission
     (existing-draftset-handler
      backend
      (restrict-to-draftset-owner backend handler)))))

(defmethod ig/pre-init-spec ::wrap-as-draftset-owner [_]
  (s/keys :req [:drafter/backend]))

(defmethod ig/init-key ::wrap-as-draftset-owner [_ opts]
  (wrap-as-draftset-owner opts))
