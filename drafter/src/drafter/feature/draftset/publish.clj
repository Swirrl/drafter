(ns drafter.feature.draftset.publish
  (:require [clojure.spec.alpha :as s]
            [drafter.rdf.draftset-management.jobs :as dsjobs]
            [drafter.responses :refer [forbidden-response submit-async-job!]]
            [drafter.user :as user]
            [drafter.util :as util]
            [integrant.core :as ig]
            [drafter.requests :as req]))

(defn handler
  [{backend :drafter/backend :keys [wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (fn [{params :params user :identity :as request}]
     (if (user/has-role? user :publisher)
       (submit-async-job!
        (dsjobs/publish-draftset-job backend
                                     (req/user-id request)
                                     params
                                     util/get-current-time))
       (forbidden-response "You require the publisher role to perform this action")))))

(defmethod ig/pre-init-spec :drafter.feature.draftset.publish/handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key :drafter.feature.draftset.publish/handler [_ opts]
  (handler opts))
