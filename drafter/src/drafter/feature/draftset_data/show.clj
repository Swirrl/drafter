(ns drafter.feature.draftset-data.show
  (:require [clojure.spec.alpha :as s]
            [drafter.backend.draftset :as ep]
            [drafter.backend.draftset.operations :as dsops]
            [drafter.middleware :as mw]
            [drafter.rdf.sparql-protocol :as sp]
            [drafter.routes.draftsets-api :refer [parse-union-with-live-handler]]
            [integrant.core :as ig]))

(defn handler
  [{wrap-as-draftset-owner :wrap-as-draftset-owner
    backend :drafter/backend
    draftset-query-timeout-fn :timeout-fn}]
  (wrap-as-draftset-owner :drafter:draft:view
   (parse-union-with-live-handler
    (fn [{{:keys [draftset-id graph union-with-live] :as params} :params :as request}]
      (let [executor (ep/build-draftset-endpoint backend draftset-id union-with-live)
            is-triples-query? (contains? params :graph)
            conneg (if is-triples-query?
                     mw/negotiate-triples-content-type-handler
                     mw/negotiate-quads-content-type-handler)
            pquery (if is-triples-query?
                     (dsops/all-graph-triples-query executor graph)
                     (dsops/all-quads-query executor))
            handler (->> sp/sparql-execution-handler
                         (sp/sparql-timeout-handler draftset-query-timeout-fn)
                         (conneg)
                         (sp/sparql-constant-prepared-query-handler pquery))]
        (handler request))))))

(defmethod ig/pre-init-spec ::handler [_]
  (s/keys :req [:drafter/backend]
          :req-un [::wrap-as-draftset-owner ::sp/timeout-fn]))

(defmethod ig/init-key ::handler [_ opts]
  (handler opts))
