(ns drafter.feature.draftset.publish
  (:require
   [clojure.spec.alpha :as s]
   [drafter.job-responses :refer [submit-async-job!]]
   [drafter.rdf.draftset-management.jobs :as dsjobs]
   [drafter.requests :as req]
   [drafter.responses :refer [forbidden-response]]
   [drafter.user :as user]
   [integrant.core :as ig]))

(defn handler
  [{manager :drafter/manager :keys [wrap-as-draftset-owner]}]
  (wrap-as-draftset-owner
   (fn [{params :params user :identity :as request}]
     (if (user/has-role? user :publisher)
       (submit-async-job!
        (dsjobs/publish-draftset-job manager
                                     (req/user-id request)
                                     params))
       (forbidden-response "You require the publisher role to perform this action")))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.publish/handler [_]
  (s/keys :req [:drafter/manager]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset.publish/handler [_ opts]
  (handler opts))
