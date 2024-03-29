(ns drafter.feature.draftset.publish
  (:require
   [clojure.spec.alpha :as s]
   [drafter.rdf.draftset-management.jobs :as dsjobs]
   [drafter.requests :as req]
   [drafter.write-scheduler :as writes]
   [integrant.core :as ig]))

(defn handler
  [{manager :drafter/manager :keys [wrap-as-draftset-owner]}]
  (wrap-as-draftset-owner :drafter:draft:publish
    (fn [{params :params :as request}]
      (writes/submit-async-job!
       (dsjobs/publish-draftset-job manager
                                    (req/user-id request)
                                    params)))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.publish/handler [_]
  (s/keys :req [:drafter/manager]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset.publish/handler [_ opts]
  (handler opts))
