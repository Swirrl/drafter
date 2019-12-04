(ns drafter.feature.draftset.create
  (:require [drafter.backend.draftset.operations :as dsops]
            [clojure.spec.alpha :as s]
            [integrant.core :as ig]
            [drafter.feature.common :as feat-common]
            [drafter.rdf.draftset-management.job-util :as jobutil]
            [drafter.util :as util]
            [ring.util.response :as ring]
            [drafter.async.responses :as response]
            [drafter.requests :as req]))

(defn create-draftsets-handler
  [{wrap-authenticated :wrap-auth
    :keys [:drafter/backend :drafter/global-writes-lock]}]
  (let [version "/v1"]
    (wrap-authenticated
     (fn [{{:keys [display-name description]} :params user :identity :as request}]
       (feat-common/run-sync
        {:backend backend :global-writes-lock global-writes-lock}
        (req/user-id request)
        'create-draftset
        nil ; because we're creating the draftset here
        #(dsops/create-draftset! backend user display-name description util/create-uuid util/get-current-time)
        (fn [result]
          (if (jobutil/failed-job-result? result)
            (response/api-response 500 result)
            (ring/redirect-after-post (str version "/draftset/"
                                           (get-in result [:details :id]))))))))))

(s/def ::wrap-auth fn?)

(defmethod ig/pre-init-spec :drafter.feature.draftset.create/handler [_]
  (s/keys :req [:drafter/backend :drafter/global-writes-lock]
          :req-un [::wrap-auth]))

(defmethod ig/init-key ::handler [_ opts]
  (create-draftsets-handler opts))
