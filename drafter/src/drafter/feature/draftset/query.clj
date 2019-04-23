(ns drafter.feature.draftset.query
  (:require [clojure.spec.alpha :as s]
            [drafter.backend :as backend]
            [drafter.feature.draftset.test-helper :as help]
            [drafter.rdf.sparql-protocol :as sp :refer [sparql-protocol-handler]]
            [integrant.core :as ig]))

(defn handler
  [{backend :drafter/backend :keys [wrap-as-draftset-owner timeout-fn]}]
  (wrap-as-draftset-owner
   (help/parse-union-with-live-handler
    (fn [{{:keys [draftset-id union-with-live]} :params :as request}]
      (let [executor (backend/endpoint-repo backend draftset-id {:union-with-live? union-with-live})
            handler (sparql-protocol-handler {:repo executor :timeout-fn timeout-fn})]
        (handler request))))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner ::sp/timeout-fn]))

(defmethod ig/init-key ::handler [_ opts]
  (handler opts))
