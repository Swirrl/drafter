(ns drafter.feature.draftset.create
  (:require
   [clojure.spec.alpha :as s]
   [drafter.backend.draftset.operations :as dsops]
   [drafter.feature.common :as feat-common]
   [drafter.middleware :as middleware]
   [drafter.rdf.draftset-management.job-util :as jobutil]
   [drafter.requests :as req]
   [drafter.responses :as response]
   [drafter.util :as util]
   [integrant.core :as ig]
   [ring.util.response :as ring]))

(defn create-draftsets-handler
  [{{:keys [backend global-writes-lock clock] :as manager} :drafter/manager
    wrap-authenticate :wrap-authenticate}]
  (let [version "/v1"]
    (middleware/wrap-authorize wrap-authenticate :drafter:draft:create
     (fn [{{:keys [display-name description]} :params user :identity :as request}]
       (feat-common/run-sync
        {:backend backend :global-writes-lock global-writes-lock}
        (req/user-id request)
        'create-draftset
        nil ; because we're creating the draftset here
        #(dsops/create-draftset! backend user display-name description util/create-uuid clock)
        (fn [result]
          (if (jobutil/failed-job-result? result)
            (response/api-response 500 result)
            (ring/redirect-after-post (str version "/draftset/"
                                           (get-in result [:details :id]))))))))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.create/handler [_]
  (s/keys :req [:drafter/manager]))

(defmethod ig/init-key ::handler [_ opts]
  (create-draftsets-handler opts))
