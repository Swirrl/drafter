(ns drafter.feature.draftset.create
  (:require [drafter.backend.draftset.operations :as dsops]
            [clojure.spec.alpha :as s]
            [integrant.core :as ig]
            [drafter.feature.common :as feat-common]
            [drafter.rdf.draftset-management.job-util :as jobutil]
            [drafter.util :as util]
            [ring.util.response :as ring]
            [swirrl-server.responses :as response]))

(defn create-draftsets-handler [{wrap-authenticated :wrap-auth backend :drafter/backend}]
  (let [version "/v1"]
    (wrap-authenticated
     (fn [{{:keys [display-name description]} :params user :identity :as request}]
       (feat-common/run-sync #(dsops/create-draftset! backend user display-name description util/create-uuid util/get-current-time)
                 (fn [result]
                   (if (jobutil/failed-job-result? result)
                     (response/api-response 500 result)
                     (ring/redirect-after-post (str version "/draftset/"
                                                    (get-in result [:details :id]))))))))))

(s/def ::wrap-auth fn?)

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-auth]))

(defmethod ig/init-key ::handler [_ opts]
  (create-draftsets-handler opts))

