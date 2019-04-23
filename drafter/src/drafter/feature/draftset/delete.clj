(ns drafter.feature.draftset.delete
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [drafter.rdf.draftset-management.jobs :as dsjobs]
            [drafter.responses :refer [submit-async-job!]]
            [integrant.core :as ig]))

(defn delete-draftset-handler
  [{wrap-as-draftset-owner :wrap-as-draftset-owner backend :drafter/backend}]
  (log/info "del draftset handler wrapper: " wrap-as-draftset-owner)
  (wrap-as-draftset-owner
   (fn [request]
     (log/info "delete-draftset-handler " request)
     (let [{{:keys [draftset-id]} :params :as request} request]
       (submit-async-job! (dsjobs/delete-draftset-job backend draftset-id))))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend] :req-un [::wrap-as-draftset-owner]))

(defmethod ig/init-key ::handler [_ opts]
  (delete-draftset-handler opts))
