(ns drafter.feature.draftset.delete
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [drafter.rdf.draftset-management.jobs :as dsjobs]
            [drafter.write-scheduler :as writes]
            [integrant.core :as ig]
            [drafter.requests :as req]))

(defn handler
  [{wrap-as-draftset-owner :wrap-as-draftset-owner backend :drafter/backend}]
  (wrap-as-draftset-owner :draft:delete
   (fn [{:keys [params] :as request}]
     (log/info "drafter.feature.draftset.delete/handler " request)
     (writes/submit-async-job!
      (dsjobs/delete-draftset-job backend (req/user-id request) params)))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::handler [_ opts]
  (handler opts))
